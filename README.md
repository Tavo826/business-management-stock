### Environment

python -m venv .venv
.\.venv\Scripts\Activate.ps1

### Dependencies

pip install -r .\requiements.txt

uvicorn app.main:app --reload

