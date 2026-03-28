from io import BytesIO
from datetime import datetime

import pandas as pd
import streamlit as st

from database import (
    add_manual_registration,
    create_template_dataframe,
    create_training_group,
    get_export_dataframe,
    get_training_groups,
    import_from_dataframe,
    init_db,
    normalize_upload_dataframe,
)

st.set_page_config(page_title="Automotive Academy Registry", layout="wide")
init_db()

st.title("Automotive Academy Participant Registry")
st.caption("Manage participants, trainings, and groups without Excel macros.")


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


with st.expander("1) Download Excel template", expanded=True):
    template_df = create_template_dataframe()
    template_bytes = dataframe_to_excel_bytes(template_df, "Template")

    st.download_button(
        label="Download Participant Template (.xlsx)",
        data=template_bytes,
        file_name="participant_upload_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.info(
        "Use comma-separated values inside 'training_programs' for participants who completed multiple trainings."
    )


with st.expander("2) Upload completed Excel file", expanded=True):
    uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])

    if uploaded_file is not None:
        try:
            raw_df = pd.read_excel(uploaded_file)
            normalized_df = normalize_upload_dataframe(raw_df)

            st.write("Preview of normalized upload")
            st.dataframe(normalized_df.head(20), use_container_width=True)

            if st.button("Import file into database", type="primary"):
                participants_count, registrations_count = import_from_dataframe(normalized_df)
                st.success(
                    f"Import complete. Processed participants: {participants_count}. "
                    f"Training registrations created/updated: {registrations_count}."
                )
        except Exception as exc:
            st.error(f"Import failed: {exc}")


left_col, right_col = st.columns(2)

with left_col:
    with st.expander("3) Register training groups", expanded=True):
        group_name = st.text_input("New training group name", placeholder="e.g., Summer-2026 Group B")
        if st.button("Create group"):
            create_training_group(group_name)
            st.success("Group saved.")

        groups_df = get_training_groups()
        if groups_df.empty:
            st.caption("No groups registered yet.")
        else:
            st.dataframe(groups_df, use_container_width=True)

with right_col:
    with st.expander("4) Manual participant registration", expanded=True):
        with st.form("manual_registration_form"):
            name = st.text_input("Name")
            surname = st.text_input("Surname")
            id_number = st.text_input("ID number")
            company = st.text_input("Company")
            role = st.text_input("Role")
            gender = st.selectbox("Gender", ["", "Female", "Male", "Other"])
            training_programs_raw = st.text_input(
                "Training programs (comma separated)",
                placeholder="Engine Diagnostics, EV Safety",
            )
            training_group = st.text_input("Training group (optional)")

            submitted = st.form_submit_button("Register participant")
            if submitted:
                try:
                    participant = {
                        "name": name.strip(),
                        "surname": surname.strip(),
                        "id_number": id_number.strip(),
                        "company": company.strip(),
                        "role": role.strip(),
                        "gender": gender.strip(),
                    }
                    programs = [p.strip() for p in training_programs_raw.split(",") if p.strip()]
                    add_manual_registration(participant, programs, training_group.strip() or None)
                    st.success("Participant registration saved.")
                except Exception as exc:
                    st.error(f"Could not save registration: {exc}")


st.markdown("---")
st.subheader("5) Export registered data")
export_df = get_export_dataframe()

if export_df.empty:
    st.caption("No registrations yet. Upload or register participants first.")
else:
    st.dataframe(export_df, use_container_width=True)

    export_bytes = dataframe_to_excel_bytes(export_df, "Registrations")
    st.download_button(
        label="Download all registrations (.xlsx)",
        data=export_bytes,
        file_name=f"academy_registrations_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
