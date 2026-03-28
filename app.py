from io import BytesIO
from datetime import datetime
import os

import pandas as pd
import streamlit as st

from database import (
    add_manual_registration,
    add_delivered_training_program,
    clear_all_registry_data,
    create_template_dataframe,
    create_delivered_programs_template_dataframe,
    delete_participant_by_id,
    delete_registration_by_id,
    delete_training_group,
    create_training_group,
    get_delivered_training_programs_dataframe,
    get_export_dataframe,
    get_participants_admin_dataframe,
    get_registrations_admin_dataframe,
    get_training_groups,
    import_delivered_training_programs_from_dataframe,
    import_from_dataframe,
    init_db,
    normalize_delivered_programs_upload_dataframe,
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
    st.caption("Advanced view of performance, trends, segmentation, and data quality.")

    if df.empty:
        st.info("No data yet. Upload or manually register participants to view analytics.")
        return

    analytics_df = df.copy()
    text_cols = [
        "training_program",
        "training_group",
        "branch",
        "direction",
        "training_status",
        "training_format",
        "gender",
        "full_name",
        "id_number",
        "company",
        "subsidiary_company",
    ]
    for col in text_cols:
        if col in analytics_df.columns:
            analytics_df[col] = analytics_df[col].fillna("").astype(str).str.strip()

    analytics_df["amount_num"] = pd.to_numeric(analytics_df.get("amount"), errors="coerce").fillna(0)
    analytics_df["start_dt"] = pd.to_datetime(analytics_df.get("start_date"), errors="coerce")
    fallback_registered = pd.to_datetime(analytics_df.get("registered_at"), errors="coerce")
    analytics_df["event_dt"] = analytics_df["start_dt"].where(analytics_df["start_dt"].notna(), fallback_registered)

    min_dt = analytics_df["event_dt"].dropna().min()
    max_dt = analytics_df["event_dt"].dropna().max()
    default_start = min_dt.date() if pd.notna(min_dt) else datetime.now().date()
    default_end = max_dt.date() if pd.notna(max_dt) else datetime.now().date()

    st.markdown('<div class="mini-card">Filters</div>', unsafe_allow_html=True)
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        date_range = st.date_input(
            "Date Range",
            value=(default_start, default_end),
        )
    with f2:
        branches = sorted([x for x in analytics_df.get("branch", pd.Series(dtype=str)).unique() if x])
        selected_branches = st.multiselect("Branch", options=branches)
    with f3:
        programs = sorted([x for x in analytics_df.get("training_program", pd.Series(dtype=str)).unique() if x])
        selected_programs = st.multiselect("Training Program", options=programs)
    with f4:
        statuses = sorted([x for x in analytics_df.get("training_status", pd.Series(dtype=str)).unique() if x])
        selected_statuses = st.multiselect("Training Status", options=statuses)

    f5, f6, f7 = st.columns(3)
    with f5:
        directions = sorted([x for x in analytics_df.get("direction", pd.Series(dtype=str)).unique() if x])
        selected_directions = st.multiselect("Direction", options=directions)
    with f6:
        formats = sorted([x for x in analytics_df.get("training_format", pd.Series(dtype=str)).unique() if x])
        selected_formats = st.multiselect("Format", options=formats)
    with f7:
        genders = sorted([x for x in analytics_df.get("gender", pd.Series(dtype=str)).unique() if x])
        selected_genders = st.multiselect("Gender", options=genders)

    filtered = analytics_df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2 and pd.notna(filtered["event_dt"]).any():
        start, end = date_range
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        filtered = filtered[filtered["event_dt"].between(start_ts, end_ts, inclusive="both")]
    if selected_branches:
        filtered = filtered[filtered["branch"].isin(selected_branches)]
    if selected_programs:
        filtered = filtered[filtered["training_program"].isin(selected_programs)]
    if selected_statuses:
        filtered = filtered[filtered["training_status"].isin(selected_statuses)]
    if selected_directions:
        filtered = filtered[filtered["direction"].isin(selected_directions)]
    if selected_formats:
        filtered = filtered[filtered["training_format"].isin(selected_formats)]
    if selected_genders:
        filtered = filtered[filtered["gender"].isin(selected_genders)]

    if filtered.empty:
        st.warning("No records match current filters. Adjust filters to continue analysis.")
        return

    complete_terms = {"completed", "\u10d3\u10d0\u10e1\u10e0\u10e3\u10da\u10d4\u10d1\u10e3\u10da\u10d8"}
    status_lower = filtered["training_status"].str.lower()
    completed_count = int(status_lower.isin(complete_terms).sum())

    total_registrations = len(filtered)
    unique_participants = filtered["id_number"].replace("", pd.NA).dropna().nunique()
    repeat_ratio = (total_registrations - unique_participants) / total_registrations if total_registrations else 0
    completion_rate = completed_count / total_registrations if total_registrations else 0
    total_revenue = float(filtered["amount_num"].sum())
    avg_revenue = float(filtered["amount_num"].mean()) if total_registrations else 0.0
    avg_trainings_per_participant = float(total_registrations / unique_participants) if unique_participants else 0.0
    grouped_registrations = int((filtered["training_group"] != "").sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Registrations", f"{total_registrations:,}")
    k2.metric("Unique Participants", f"{unique_participants:,}")
    k3.metric("Total Revenue", f"{total_revenue:,.2f}")
    k4.metric("Avg Revenue / Registration", f"{avg_revenue:,.2f}")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Completion Rate", f"{completion_rate:.1%}")
    k6.metric("Repeat Registration Rate", f"{repeat_ratio:.1%}")
    k7.metric("Avg Trainings / Participant", f"{avg_trainings_per_participant:.2f}")
    k8.metric("Grouped Registrations", f"{grouped_registrations:,}")

    # period-over-period comparison against immediate previous equal-length window
    if pd.notna(filtered["event_dt"]).any():
        period_start = filtered["event_dt"].min().normalize()
        period_end = filtered["event_dt"].max().normalize()
        days = int((period_end - period_start).days) + 1
        prev_end = period_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=max(days - 1, 0))
        previous = analytics_df[analytics_df["event_dt"].between(prev_start, prev_end, inclusive="both")]
        prev_regs = len(previous)
        prev_revenue = float(previous["amount_num"].sum()) if prev_regs else 0.0
        prev_delta_regs = total_registrations - prev_regs
        prev_delta_rev = total_revenue - prev_revenue
        p1, p2 = st.columns(2)
        p1.metric("Registrations vs Previous Period", f"{total_registrations:,}", delta=f"{prev_delta_regs:+,}")
        p2.metric("Revenue vs Previous Period", f"{total_revenue:,.2f}", delta=f"{prev_delta_rev:+,.2f}")

    tab_trends, tab_segments, tab_quality = st.tabs(["Trends", "Segments", "Data Quality"])

    with tab_trends:
        trend_left, trend_right = st.columns(2)
        with trend_left:
            st.markdown('<div class="mini-card">Monthly Registrations</div>', unsafe_allow_html=True)
            monthly_regs = (
                filtered.dropna(subset=["event_dt"])
                .assign(month=lambda x: x["event_dt"].dt.to_period("M").astype(str))
                .groupby("month", as_index=False)
                .size()
                .rename(columns={"size": "registrations"})
                .set_index("month")
            )
            if monthly_regs.empty:
                st.caption("No valid dates for trend analysis.")
            else:
                st.line_chart(monthly_regs)
        with trend_right:
            st.markdown('<div class="mini-card">Monthly Revenue</div>', unsafe_allow_html=True)
            monthly_rev = (
                filtered.dropna(subset=["event_dt"])
                .assign(month=lambda x: x["event_dt"].dt.to_period("M").astype(str))
                .groupby("month", as_index=False)["amount_num"]
                .sum()
                .rename(columns={"amount_num": "revenue"})
                .set_index("month")
            )
            if monthly_rev.empty:
                st.caption("No valid revenue dates for trend analysis.")
            else:
                st.area_chart(monthly_rev)

        st.markdown('<div class="mini-card">Top Programs by Volume and Revenue</div>', unsafe_allow_html=True)
        program_perf = (
            filtered.groupby("training_program", as_index=False)
            .agg(
                registrations=("training_program", "size"),
                unique_participants=("id_number", "nunique"),
                revenue=("amount_num", "sum"),
            )
            .sort_values(["registrations", "revenue"], ascending=False)
            .head(20)
        )
        st.dataframe(program_perf, use_container_width=True, hide_index=True)

    with tab_segments:
        seg1, seg2 = st.columns(2)
        with seg1:
            st.markdown('<div class="mini-card">Branch Performance</div>', unsafe_allow_html=True)
            branch_perf = (
                filtered[filtered["branch"] != ""]
                .groupby("branch", as_index=False)
                .agg(
                    registrations=("branch", "size"),
                    participants=("id_number", "nunique"),
                    revenue=("amount_num", "sum"),
                )
                .sort_values("registrations", ascending=False)
                .head(15)
                .set_index("branch")
            )
            if branch_perf.empty:
                st.caption("No branch data available.")
            else:
                st.bar_chart(branch_perf[["registrations"]])
                st.dataframe(branch_perf.reset_index(), use_container_width=True, hide_index=True)
        with seg2:
            st.markdown('<div class="mini-card">Direction Performance</div>', unsafe_allow_html=True)
            direction_perf = (
                filtered[filtered["direction"] != ""]
                .groupby("direction", as_index=False)
                .agg(
                    registrations=("direction", "size"),
                    participants=("id_number", "nunique"),
                    revenue=("amount_num", "sum"),
                )
                .sort_values("registrations", ascending=False)
                .head(15)
                .set_index("direction")
            )
            if direction_perf.empty:
                st.caption("No direction data available.")
            else:
                st.bar_chart(direction_perf[["registrations"]])
                st.dataframe(direction_perf.reset_index(), use_container_width=True, hide_index=True)

        seg3, seg4 = st.columns(2)
        with seg3:
            st.markdown('<div class="mini-card">Status Mix</div>', unsafe_allow_html=True)
            status_mix = (
                filtered[filtered["training_status"] != ""]
                .groupby("training_status", as_index=False)
                .size()
                .rename(columns={"size": "registrations"})
                .sort_values("registrations", ascending=False)
                .set_index("training_status")
            )
            if status_mix.empty:
                st.caption("No status data available.")
            else:
                st.bar_chart(status_mix)
        with seg4:
            st.markdown('<div class="mini-card">Format Mix</div>', unsafe_allow_html=True)
            format_mix = (
                filtered[filtered["training_format"] != ""]
                .groupby("training_format", as_index=False)
                .size()
                .rename(columns={"size": "registrations"})
                .sort_values("registrations", ascending=False)
                .set_index("training_format")
            )
            if format_mix.empty:
                st.caption("No format data available.")
            else:
                st.bar_chart(format_mix)

        st.markdown('<div class="mini-card">Top Participants by Number of Trainings</div>', unsafe_allow_html=True)
        participant_load = (
            filtered[filtered["id_number"] != ""]
            .groupby(["id_number", "full_name"], as_index=False)
            .agg(
                trainings=("training_program", "size"),
                revenue=("amount_num", "sum"),
            )
            .sort_values("trainings", ascending=False)
            .head(25)
        )
        st.dataframe(participant_load, use_container_width=True, hide_index=True)

    with tab_quality:
        st.markdown('<div class="mini-card">Field Completeness</div>', unsafe_allow_html=True)
        completeness_cols = [
            "id_number",
            "full_name",
            "training_program",
            "training_code",
            "start_date",
            "end_date",
            "training_status",
            "training_format",
            "branch",
            "direction",
            "amount_num",
        ]
        available_cols = [c for c in completeness_cols if c in filtered.columns]
        completeness_rows = []
        for col in available_cols:
            if col == "amount_num":
                missing = int((filtered[col] == 0).sum())
            else:
                missing = int(filtered[col].isna().sum() + (filtered[col].astype(str).str.strip() == "").sum())
            completeness_rows.append(
                {
                    "field": col,
                    "missing_records": missing,
                    "missing_percent": round(missing / len(filtered) * 100, 2),
                }
            )
        completeness_df = pd.DataFrame(completeness_rows).sort_values("missing_percent", ascending=False)
        st.dataframe(completeness_df, use_container_width=True, hide_index=True)

        q1, q2 = st.columns(2)
        with q1:
            st.markdown('<div class="mini-card">Potential ID Quality Issues</div>', unsafe_allow_html=True)
            invalid_ids = filtered[
                (filtered["id_number"].astype(str).str.strip() == "")
                | (~filtered["id_number"].astype(str).str.fullmatch(r"[0-9]{11}", na=False))
            ]
            st.metric("Potentially Invalid IDs", f"{len(invalid_ids):,}")
            if not invalid_ids.empty:
                st.dataframe(
                    invalid_ids[["id_number", "full_name", "training_program"]].head(20),
                    use_container_width=True,
                    hide_index=True,
                )
        with q2:
            st.markdown('<div class="mini-card">Duplicate Name Per ID Check</div>', unsafe_allow_html=True)
            duplicates = (
                filtered[filtered["id_number"] != ""]
                .groupby("id_number")["full_name"]
                .nunique()
                .reset_index(name="distinct_names")
            )
            duplicates = duplicates[duplicates["distinct_names"] > 1].sort_values("distinct_names", ascending=False)
            st.metric("IDs Linked to Multiple Names", f"{len(duplicates):,}")
            if not duplicates.empty:
                st.dataframe(duplicates.head(20), use_container_width=True, hide_index=True)


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


def render_delivered_programs_section() -> None:
    st.subheader("Conducted Training Programs")
    st.caption("Register delivered programs, upload in bulk, and analyze delivery performance.")

    delivered_df = get_delivered_training_programs_dataframe()
    subtab_register, subtab_bulk, subtab_analytics = st.tabs(
        ["Register Program", "Bulk Upload", "Analytics"]
    )

    with subtab_register:
        with st.form("delivered_program_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                training_type = st.text_input("Type")
                direction = st.text_input("Direction")
                training_name = st.text_input("Training Name")
                training_format = st.text_input("Format")
                sale_type = st.text_input("Sale Type")
            with c2:
                client_company = st.text_input("Client / Company")
                start_date = st.text_input("Start Date", placeholder="2026-03-01")
                end_date = st.text_input("End Date", placeholder="2026-03-03")
                duration = st.number_input("Duration (hours)", min_value=0.0, step=1.0, value=0.0)
            with c3:
                trainer = st.text_input("Trainer")
                participants_count = st.number_input("Number of Participants", min_value=0, step=1, value=0)
                satis_rate = st.number_input("Satis. Survey Rate", min_value=0.0, max_value=1.0, step=0.01, value=0.0)
                revenue = st.number_input("Revenue", min_value=0.0, step=1.0, value=0.0)
                branch = st.text_input("Branch")

            if st.form_submit_button("Save Delivered Program", type="primary"):
                try:
                    payload = {
                        "type": training_type.strip(),
                        "direction": direction.strip(),
                        "training_name": training_name.strip(),
                        "format": training_format.strip(),
                        "sale_type": sale_type.strip(),
                        "client_company": client_company.strip(),
                        "start_date": start_date.strip(),
                        "end_date": end_date.strip(),
                        "duration": duration,
                        "trainer": trainer.strip(),
                        "number_of_participants": participants_count,
                        "satis_survey_rate": satis_rate,
                        "revenue": revenue,
                        "branch": branch.strip(),
                    }
                    add_delivered_training_program(payload)
                    st.success("Delivered training program saved.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not save record: {exc}")

        st.dataframe(delivered_df, use_container_width=True, hide_index=True)

    with subtab_bulk:
        template_df = create_delivered_programs_template_dataframe()
        template_bytes = dataframe_to_excel_bytes(template_df, "Conducted_Programs_Template")
        st.download_button(
            label="Download Bulk Upload Template (.xlsx)",
            data=template_bytes,
            file_name="conducted_training_programs_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.caption(
            "Expected headers: Type, direction, training name, format, sale type, client/company, "
            "start date, end date, duration, trainer, number of participants, satis. survey rate, revenue, branch."
        )

        uploaded = st.file_uploader(
            "Upload Conducted Programs Excel",
            type=["xlsx", "xls"],
            key="conducted_programs_upload",
        )
        if uploaded is not None:
            try:
                raw_df = pd.read_excel(uploaded)
                normalized_df = normalize_delivered_programs_upload_dataframe(raw_df)
                st.dataframe(normalized_df.head(30), use_container_width=True)
                if st.button("Import Conducted Programs", type="primary"):
                    inserted = import_delivered_training_programs_from_dataframe(normalized_df)
                    st.success(f"Import complete. Inserted records: {inserted}.")
                    st.rerun()
            except Exception as exc:
                st.error(f"Import failed: {exc}")

    with subtab_analytics:
        st.markdown('<div class="mini-card">Conducted Programs Analytics</div>', unsafe_allow_html=True)
        if delivered_df.empty:
            st.info("No conducted program records yet.")
            return

        analytics = delivered_df.copy()
        for col in ["type", "direction", "training_name", "format", "sale_type", "client_company", "trainer", "branch"]:
            if col in analytics.columns:
                analytics[col] = analytics[col].fillna("").astype(str).str.strip()
        analytics["start_dt"] = pd.to_datetime(analytics.get("start_date"), errors="coerce")
        analytics["end_dt"] = pd.to_datetime(analytics.get("end_date"), errors="coerce")
        analytics["duration_num"] = pd.to_numeric(analytics.get("duration"), errors="coerce").fillna(0)
        analytics["participants_num"] = pd.to_numeric(analytics.get("number_of_participants"), errors="coerce").fillna(0)
        analytics["satis_num"] = pd.to_numeric(analytics.get("satis_survey_rate"), errors="coerce")
        analytics["revenue_num"] = pd.to_numeric(analytics.get("revenue"), errors="coerce").fillna(0)

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Programs Delivered", f"{len(analytics):,}")
        a2.metric("Total Participants", f"{int(analytics['participants_num'].sum()):,}")
        a3.metric("Total Revenue", f"{float(analytics['revenue_num'].sum()):,.2f}")
        a4.metric("Avg Satisfaction Rate", f"{float(analytics['satis_num'].mean()):.2%}" if analytics["satis_num"].notna().any() else "N/A")

        b1, b2, b3 = st.columns(3)
        b1.metric("Avg Participants / Program", f"{float(analytics['participants_num'].mean()):.2f}")
        b2.metric("Avg Duration (hours)", f"{float(analytics['duration_num'].mean()):.2f}")
        b3.metric("Revenue / Participant", f"{float(analytics['revenue_num'].sum() / max(analytics['participants_num'].sum(), 1)):.2f}")

        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown('<div class="mini-card">Top Training Names</div>', unsafe_allow_html=True)
            top_names = (
                analytics[analytics["training_name"] != ""]
                .groupby("training_name", as_index=False)
                .agg(programs=("training_name", "size"), revenue=("revenue_num", "sum"))
                .sort_values(["programs", "revenue"], ascending=False)
                .head(12)
                .set_index("training_name")
            )
            if top_names.empty:
                st.caption("No training names found.")
            else:
                st.bar_chart(top_names[["programs"]])

        with ch2:
            st.markdown('<div class="mini-card">Revenue by Branch</div>', unsafe_allow_html=True)
            branch_perf = (
                analytics[analytics["branch"] != ""]
                .groupby("branch", as_index=False)["revenue_num"]
                .sum()
                .rename(columns={"revenue_num": "revenue"})
                .sort_values("revenue", ascending=False)
                .head(12)
                .set_index("branch")
            )
            if branch_perf.empty:
                st.caption("No branch data found.")
            else:
                st.bar_chart(branch_perf)

        ch3, ch4 = st.columns(2)
        with ch3:
            st.markdown('<div class="mini-card">Monthly Delivery Trend</div>', unsafe_allow_html=True)
            monthly = (
                analytics.dropna(subset=["start_dt"])
                .assign(month=lambda x: x["start_dt"].dt.to_period("M").astype(str))
                .groupby("month", as_index=False)
                .agg(programs=("month", "size"), participants=("participants_num", "sum"), revenue=("revenue_num", "sum"))
                .set_index("month")
            )
            if monthly.empty:
                st.caption("No valid dates for trend analysis.")
            else:
                st.line_chart(monthly[["programs", "participants"]])
        with ch4:
            st.markdown('<div class="mini-card">Satisfaction by Trainer</div>', unsafe_allow_html=True)
            trainer_satis = (
                analytics[(analytics["trainer"] != "") & analytics["satis_num"].notna()]
                .groupby("trainer", as_index=False)
                .agg(avg_satisfaction=("satis_num", "mean"), programs=("trainer", "size"))
                .sort_values("avg_satisfaction", ascending=False)
                .head(12)
            )
            if trainer_satis.empty:
                st.caption("No trainer satisfaction data available.")
            else:
                st.dataframe(trainer_satis, use_container_width=True, hide_index=True)

        st.markdown('<div class="mini-card">Detailed Conducted Programs Table</div>', unsafe_allow_html=True)
        st.dataframe(analytics, use_container_width=True, hide_index=True)


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
    delivered_export = get_delivered_training_programs_dataframe()
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        participants_export.to_excel(writer, index=False, sheet_name="Participants")
        registrations_export.to_excel(writer, index=False, sheet_name="Registrations")
        delivered_export.to_excel(writer, index=False, sheet_name="Conducted_Programs")
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
tab_dashboard, tab_import, tab_manual, tab_groups, tab_export, tab_conducted, tab_admin = st.tabs(
    ["Dashboard", "Bulk Import", "Manual Entry", "Groups", "Export", "Conducted Programs", "Admin Panel"]
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

with tab_conducted:
    render_delivered_programs_section()

with tab_admin:
    render_admin_panel()
