# Report generators

Word documents built from the analysis outputs, so every number in a report traces back to data.

```bash
cd reports && npm install
```

```bash
python report_data.py .. report_data.json "<one-sentence season note>" <number of tests>
```

```bash
node status_report.js report_data.json ../data/out/overview/season_2020_compact.png ../../SmokeStream_Status_Report.docx
```

`est.py` estimates the page count of a .docx (open it in Word to confirm).
