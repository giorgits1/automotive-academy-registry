# Automotive Academy Registry App

This app replaces macro-heavy Excel workflows with a browser-based system.

## What staff need
Nothing to install. Staff only need a web link and can use the app from any browser.

## Features
- Download an Excel template for participant imports.
- Upload completed Excel files and auto-register participants.
- Support multiple trainings per participant (comma-separated in one cell).
- Create and manage training groups.
- Register participants manually.
- Export all registered data to Excel.

## Upload format
Required columns:
- `name`
- `surname`
- `id_number`
- `training_programs`

Optional columns:
- `company`
- `role`
- `gender`
- `training_group`

## One-time deployment (Render)
1. Create a GitHub repository and push this project.
2. In Render, create a new **Blueprint** service from the repository.
3. Render will read `render.yaml` and create:
   - a web service running Streamlit
   - a persistent disk for your SQLite database
4. After deploy completes, copy the public app URL.
5. Share that URL with your staff.

The app is already configured for hosting:
- `render.yaml` defines build/start commands.
- `DB_PATH=/var/data/academy.db` stores data on persistent disk.

## Local run (optional)
If you want to test locally before deployment:
1. Install Python 3.11+.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run:
   ```bash
   streamlit run app.py
   ```

## Notes
- The database is created automatically.
- Same participant can be linked to multiple training programs.
- You can create training groups and reuse them during imports/manual registration.
