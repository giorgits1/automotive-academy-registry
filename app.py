from io import BytesIO
from datetime import datetime
import os

import pandas as pd
import streamlit as st

from database import (
    add_manual_registration,
    clear_all_registry_data,
    create_template_dataframe,
    delete_participant_by_id,
    delete_registration_by_id,
    delete_training_group,
    create_training_group,
    get_export_dataframe,
    get_participants_admin_dataframe,
    get_registrations_admin_dataframe,
    get_training_groups,
    import_from_dataframe,
    init_db,
    normalize_upload_dataframe,
    update_participant_by_id,
    update_registration_by_id,
)

st.set_page_config(page_title="Automotive Academy Registry", layout="wide")
init_db()

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&display=swap');

    html, body, [class*="css"]  {
        font-family: 'Manrope', sans-serif;
    }

    .stApp {
        background:
            radial-gradient(circle at 10% 20%, rgba(34,197,94,0.14), transparent 40%),
            radial-gradient(circle at 85% 10%, rgba(245,158,11,0.18), transparent 35%),
            linear-gradient(135deg, #f5f7fa 0%, #eef2f5 100%);
    }

    .header-card {
        background: #0f172a;
        border-radius: 18px;
        padding: 1.1rem 1.4rem;
        color: #f8fafc;
        box-shadow: 0 8px 22px rgba(2,6,23,.18);
        animation: fadeIn .6s ease-out;
    }

    .mini-card {
        background: white;
        border-radius: 14px;
        padding: .9rem 1rem;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.06);
        animation: fadeIn .5s ease-out;
    }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(8px); }
        to { opacity: 1; transform: translateY(0); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="header-card">
        <h2 style="margin:0;">Automotive Academy Command Center</h2>
        <p style="margin:.3rem 0 0 0; color:#cbd5e1;">
            Upload participants, manage training groups, and track academy performance in one place.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def admin_auth_gate() -> bool:
    st.subheader("Admin Access")
    st.caption("Sign in to manage participants, registrations, and groups.")

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    if admin_password == "admin123":
        st.warning("Default admin password is active (`admin123`). Set `ADMIN_PASSWORD` in deployment settings.")

    if st.session_state.admin_authenticated:
        left, right = st.columns([3, 1])
        left.success("Admin session active.")
        if right.button("Logout", use_container_width=True):
            st.session_state.admin_authenticated = False
            st.rerun()
        return True

    with st.form("admin_login_form", clear_on_submit=True):
        password = st.text_input("Admin Password", type="password")
        submitted = st.form_submit_button("Sign In", type="primary")
        if submitted:
            if password == admin_password:
                st.session_state.admin_authenticated = True
                st.success("Signed in successfully.")
                st.rerun()
            else:
                st.error("Invalid password.")
    return False


def render_dashboard(df: pd.DataFrame) -> None:
    st.subheader("Analytics Dashboard")
    st.caption("Live overview of registrations, trends, and training performance.")

    if df.empty:
        st.info("No data yet. Upload or manually register participants to view analytics.")
        return

    total_registrations = len(df)
    unique_participants = df["id_number"].nunique()
    total_programs = df["training_program"].nunique()
    total_groups = (df["training_group"].fillna("").str.strip() != "").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Registrations", f"{total_registrations:,}")
    c2.metric("Unique Participants", f"{unique_participants:,}")
    c3.metric("Training Programs", f"{total_programs:,}")
    c4.metric("Grouped Registrations", f"{total_groups:,}")

    charts_col_1, charts_col_2 = st.columns(2)

    with charts_col_1:
        st.markdown('<div class="mini-card">Top Training Programs</div>', unsafe_allow_html=True)
        top_programs = (
            df.groupby("training_program", as_index=False)
            .size()
            .rename(columns={"size": "registrations"})
            .sort_values("registrations", ascending=False)
            .head(10)
            .set_index("training_program")
        )
        st.bar_chart(top_programs)

    with charts_col_2:
        st.markdown('<div class="mini-card">Top Companies</div>', unsafe_allow_html=True)
        company_source = df["company"].fillna("").astype(str).str.strip()
        if "subsidiary_company" in df.columns:
            subsidiary_source = df["subsidiary_company"].fillna("").astype(str).str.strip()
            company_source = company_source.where(company_source != "", subsidiary_source)
        top_companies = (
            pd.DataFrame({"company": company_source})
            .query("company != ''")
            .groupby("company", as_index=False)
            .size()
            .rename(columns={"size": "registrations"})
            .sort_values("registrations", ascending=False)
            .head(10)
            .set_index("company")
        )
        if top_companies.empty:
            st.caption("No company data available yet.")
        else:
            st.bar_chart(top_companies)

    trend_col, mix_col = st.columns(2)
    with trend_col:
        st.markdown('<div class="mini-card">Monthly Registration Trend</div>', unsafe_allow_html=True)
        trend_df = df.copy()
        trend_df["registered_at"] = pd.to_datetime(trend_df["registered_at"], errors="coerce")
        trend_df["month"] = trend_df["registered_at"].dt.to_period("M").astype(str)
        monthly = (
            trend_df.dropna(subset=["registered_at"])
            .groupby("month", as_index=False)
            .size()
            .rename(columns={"size": "registrations"})
            .set_index("month")
        )
        if monthly.empty:
            st.caption("Not enough timestamp data yet.")
        else:
            st.line_chart(monthly)

    with mix_col:
        st.markdown('<div class="mini-card">Gender Distribution</div>', unsafe_allow_html=True)
        gender_mix = (
            df[df["gender"].fillna("").str.strip() != ""]
            .groupby("gender", as_index=False)
            .size()
            .rename(columns={"size": "registrations"})
            .set_index("gender")
        )
        if gender_mix.empty:
            st.caption("No gender values entered yet.")
        else:
            st.bar_chart(gender_mix)

    group_breakdown = (
        df[df["training_group"].fillna("").str.strip() != ""]
        .groupby("training_group", as_index=False)
        .size()
        .rename(columns={"size": "registrations"})
        .sort_values("registrations", ascending=False)
    )
    st.markdown('<div class="mini-card">Training Group Performance</div>', unsafe_allow_html=True)
    if group_breakdown.empty:
        st.caption("No training groups assigned yet.")
    else:
        st.dataframe(group_breakdown, use_container_width=True)


def render_import_section() -> None:
    st.subheader("Bulk Import")
    st.caption("Use template-driven Excel uploads for fast participant registration.")

    template_df = create_template_dataframe()
    template_bytes = dataframe_to_excel_bytes(template_df, "Template")
    st.download_button(
        label="Download Excel Template",
        data=template_bytes,
        file_name="participant_upload_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.caption(
        "Supports both your existing Georgian headers (e.g. `საიდ.კოდი`, `ტრენინგის კოდი`, `დაწყება`) "
        "and English template headers."
    )
    st.caption("For multiple trainings, separate names with commas in `ტრენინგის დასახელება` column.")

    uploaded_file = st.file_uploader("Upload completed participant Excel", type=["xlsx", "xls"])
    if uploaded_file is None:
        return

    try:
        raw_df = pd.read_excel(uploaded_file)
        normalized_df = normalize_upload_dataframe(raw_df)
        st.dataframe(normalized_df.head(25), use_container_width=True)

        if st.button("Import Into Database", type="primary"):
            participants_count, registrations_count = import_from_dataframe(normalized_df)
            st.success(
                f"Import complete. Processed participants: {participants_count}. "
                f"Training registrations created/updated: {registrations_count}."
            )
    except Exception as exc:
        st.error(f"Import failed: {exc}")


def render_manual_registration() -> None:
    st.subheader("Manual Registration")
    st.caption("Register a participant and assign one or more trainings instantly.")

    with st.form("manual_registration_form", clear_on_submit=True):
        col_1, col_2, col_3 = st.columns(3)
        with col_1:
            name = st.text_input("Name")
            surname = st.text_input("Surname")
            full_name = st.text_input("Full Name")
            id_number = st.text_input("ID number")
            company = st.text_input("Company")
        with col_2:
            subsidiary_company = st.text_input("Subsidiary Company")
            role = st.text_input("Role / Position")
            position = st.text_input("Position")
            position_type = st.text_input("Position Type")
            division = st.text_input("Division")
        with col_3:
            department = st.text_input("Department")
            direction = st.text_input("Direction")
            branch = st.text_input("Branch")
            gender = st.selectbox("Gender", ["", "Female", "Male", "Other"])
            training_group = st.text_input("Training group (optional)")

        st.markdown("Training Details")
        td_1, td_2, td_3 = st.columns(3)
        with td_1:
            training_programs_raw = st.text_input(
                "Training programs (comma separated)",
                placeholder="Engine Diagnostics, EV Safety",
            )
            training_code = st.text_input("Training Code")
        with td_2:
            training_format = st.text_input("Training Format", placeholder="F2F / Online")
            training_status = st.text_input("Training Status", placeholder="Completed")
        with td_3:
            start_date = st.text_input("Start Date", placeholder="2026-03-25")
            end_date = st.text_input("End Date", placeholder="2026-03-25")
            amount = st.number_input("Amount", min_value=0.0, step=1.0, value=0.0)

        submitted = st.form_submit_button("Save Registration", type="primary")
        if submitted:
            try:
                participant = {
                    "name": name.strip(),
                    "surname": surname.strip(),
                    "id_number": id_number.strip(),
                    "full_name": full_name.strip(),
                    "company": company.strip(),
                    "subsidiary_company": subsidiary_company.strip(),
                    "role": role.strip(),
                    "position": position.strip(),
                    "position_type": position_type.strip(),
                    "division": division.strip(),
                    "department": department.strip(),
                    "direction": direction.strip(),
                    "branch": branch.strip(),
                    "gender": gender.strip(),
                    "training_code": training_code.strip(),
                    "training_format": training_format.strip(),
                    "training_status": training_status.strip(),
                    "start_date": start_date.strip(),
                    "end_date": end_date.strip(),
                    "amount": float(amount) if amount else None,
                }
                programs = [p.strip() for p in training_programs_raw.split(",") if p.strip()]
                add_manual_registration(participant, programs, training_group.strip() or None)
                st.success("Participant registration saved.")
            except Exception as exc:
                st.error(f"Could not save registration: {exc}")


def render_groups() -> None:
    st.subheader("Training Groups")
    st.caption("Create groups and monitor registration volume by cohort.")

    group_name = st.text_input("New training group name", placeholder="e.g., Summer-2026 Group B")
    if st.button("Create Group"):
        create_training_group(group_name)
        st.success("Group saved.")

    groups_df = get_training_groups()
    if groups_df.empty:
        st.caption("No groups registered yet.")
        return
    st.dataframe(groups_df, use_container_width=True)


def render_export(df: pd.DataFrame) -> None:
    st.subheader("Data Export")
    st.caption("Download all registrations at any time for reporting.")

    if df.empty:
        st.caption("No registrations yet. Upload or register participants first.")
        return

    st.dataframe(df, use_container_width=True)

    export_bytes = dataframe_to_excel_bytes(df, "Registrations")
    st.download_button(
        label="Download Full Registration Report (.xlsx)",
        data=export_bytes,
        file_name=f"academy_registrations_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


def render_admin_panel() -> None:
    if not admin_auth_gate():
        return

    def safe_text(value: object) -> str:
        if value is None:
            return ""
        if pd.isna(value):
            return ""
        return str(value)

    st.markdown("---")
    st.subheader("Participant Manager")
    participants_df = get_participants_admin_dataframe()
    participant_search = st.text_input("Search participant (name or ID)")
    if participant_search.strip():
        p_mask = (
            participants_df["id_number"].astype(str).str.contains(participant_search, case=False, na=False)
            | participants_df["full_name"].astype(str).str.contains(participant_search, case=False, na=False)
        )
        participants_view = participants_df[p_mask].copy()
    else:
        participants_view = participants_df.copy()

    st.dataframe(participants_view, use_container_width=True, hide_index=True)
    if not participants_view.empty:
        participant_options = participants_view["participant_id"].tolist()
        selected_pid = st.selectbox("Select participant ID to edit", options=participant_options)
        selected_row = participants_df[participants_df["participant_id"] == selected_pid].iloc[0]

        with st.form("participant_edit_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                full_name = st.text_input("Full Name", value=safe_text(selected_row["full_name"]))
                id_number = st.text_input("ID Number", value=safe_text(selected_row["id_number"]))
                name = st.text_input("Name", value=safe_text(selected_row["name"]))
                surname = st.text_input("Surname", value=safe_text(selected_row["surname"]))
                gender = st.text_input("Gender", value=safe_text(selected_row["gender"]))
            with c2:
                company = st.text_input("Company", value=safe_text(selected_row["company"]))
                subsidiary_company = st.text_input("Subsidiary Company", value=safe_text(selected_row["subsidiary_company"]))
                role = st.text_input("Role", value=safe_text(selected_row["role"]))
                position = st.text_input("Position", value=safe_text(selected_row["position"]))
                position_type = st.text_input("Position Type", value=safe_text(selected_row["position_type"]))
            with c3:
                division = st.text_input("Division", value=safe_text(selected_row["division"]))
                department = st.text_input("Department", value=safe_text(selected_row["department"]))
                direction = st.text_input("Direction", value=safe_text(selected_row["direction"]))
                branch = st.text_input("Branch", value=safe_text(selected_row["branch"]))

            if st.form_submit_button("Update Participant", type="primary"):
                payload = {
                    "full_name": full_name.strip(),
                    "id_number": id_number.strip(),
                    "name": name.strip() or "-",
                    "surname": surname.strip() or "-",
                    "gender": gender.strip(),
                    "company": company.strip(),
                    "subsidiary_company": subsidiary_company.strip(),
                    "role": role.strip(),
                    "position": position.strip(),
                    "position_type": position_type.strip(),
                    "division": division.strip(),
                    "department": department.strip(),
                    "direction": direction.strip(),
                    "branch": branch.strip(),
                }
                update_participant_by_id(int(selected_pid), payload)
                st.success("Participant updated.")
                st.rerun()

        confirm_delete_participant = st.checkbox("I confirm deleting this participant and all related registrations")
        if st.button("Delete Participant", type="secondary", use_container_width=False) and confirm_delete_participant:
            delete_participant_by_id(int(selected_pid))
            st.success("Participant deleted.")
            st.rerun()

    st.markdown("---")
    st.subheader("Registration Manager")
    registrations_df = get_registrations_admin_dataframe()
    reg_search = st.text_input("Search registration (name, training, code)")
    if reg_search.strip():
        r_mask = (
            registrations_df["full_name"].astype(str).str.contains(reg_search, case=False, na=False)
            | registrations_df["training_program"].astype(str).str.contains(reg_search, case=False, na=False)
            | registrations_df["training_code"].astype(str).str.contains(reg_search, case=False, na=False)
        )
        registrations_view = registrations_df[r_mask].copy()
    else:
        registrations_view = registrations_df.copy()

    st.dataframe(registrations_view, use_container_width=True, hide_index=True)
    if not registrations_view.empty:
        selected_rid = st.selectbox(
            "Select registration ID to edit",
            options=registrations_view["registration_id"].tolist(),
        )
        selected_registration = registrations_df[registrations_df["registration_id"] == selected_rid].iloc[0]

        with st.form("registration_edit_form"):
            rc1, rc2, rc3 = st.columns(3)
            with rc1:
                training_program = st.text_input("Training Program", value=safe_text(selected_registration["training_program"]))
                training_group = st.text_input("Training Group", value=safe_text(selected_registration["training_group"]))
                training_code = st.text_input("Training Code", value=safe_text(selected_registration["training_code"]))
            with rc2:
                training_format = st.text_input("Training Format", value=safe_text(selected_registration["training_format"]))
                training_status = st.text_input("Training Status", value=safe_text(selected_registration["training_status"]))
                start_date = st.text_input("Start Date", value=safe_text(selected_registration["start_date"]))
            with rc3:
                end_date = st.text_input("End Date", value=safe_text(selected_registration["end_date"]))
                amount = st.text_input("Amount", value=safe_text(selected_registration["amount"]))

            if st.form_submit_button("Update Registration", type="primary"):
                payload = {
                    "training_program": training_program.strip(),
                    "training_group": training_group.strip(),
                    "training_code": training_code.strip(),
                    "training_format": training_format.strip(),
                    "training_status": training_status.strip(),
                    "start_date": start_date.strip(),
                    "end_date": end_date.strip(),
                    "amount": amount.strip(),
                }
                update_registration_by_id(int(selected_rid), payload)
                st.success("Registration updated.")
                st.rerun()

        confirm_delete_registration = st.checkbox("I confirm deleting this registration")
        if st.button("Delete Registration", type="secondary") and confirm_delete_registration:
            delete_registration_by_id(int(selected_rid))
            st.success("Registration deleted.")
            st.rerun()

    st.markdown("---")
    st.subheader("Group Manager")
    groups_df = get_training_groups()
    st.dataframe(groups_df, use_container_width=True, hide_index=True)
    if not groups_df.empty:
        selected_group_name = st.selectbox("Select group to delete", options=groups_df["group_name"].tolist())
        confirm_delete_group = st.checkbox("I confirm deleting selected group")
        if st.button("Delete Group", type="secondary") and confirm_delete_group:
            delete_training_group(selected_group_name)
            st.success("Group deleted.")
            st.rerun()

    st.markdown("---")
    st.subheader("Admin Export")
    participants_export = get_participants_admin_dataframe()
    registrations_export = get_registrations_admin_dataframe()
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        participants_export.to_excel(writer, index=False, sheet_name="Participants")
        registrations_export.to_excel(writer, index=False, sheet_name="Registrations")
    st.download_button(
        "Download Admin Workbook (.xlsx)",
        data=output.getvalue(),
        file_name=f"academy_admin_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.markdown("---")
    st.subheader("Danger Zone")
    st.caption("Permanently delete all participants, registrations, programs, and groups.")
    confirm_clear = st.checkbox("I understand this action cannot be undone")
    confirm_text = st.text_input("Type CLEAR to confirm full registry reset")
    if st.button("Clear All Registry Data", type="secondary"):
        if not confirm_clear or confirm_text.strip().upper() != "CLEAR":
            st.error("Please confirm by checking the box and typing CLEAR.")
        else:
            clear_all_registry_data()
            st.success("All registry data has been deleted.")
            st.rerun()


export_df = get_export_dataframe()
tab_dashboard, tab_import, tab_manual, tab_groups, tab_export, tab_admin = st.tabs(
    ["Dashboard", "Bulk Import", "Manual Entry", "Groups", "Export", "Admin Panel"]
)

with tab_dashboard:
    render_dashboard(export_df)

with tab_import:
    render_import_section()

with tab_manual:
    render_manual_registration()

with tab_groups:
    render_groups()

with tab_export:
    render_export(export_df)

with tab_admin:
    render_admin_panel()
