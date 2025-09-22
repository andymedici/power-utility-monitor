from flask import Flask, render_template_string
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Database connection settings
DATABASE_URL = os.environ.get("DATABASE_URL")  # Railway provides this in env vars

HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Scan Results</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    h1 { color: #2c3e50; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ccc; padding: 8px; }
    th { background-color: #f4f4f4; }
  </style>
</head>
<body>
  <h1>Latest Scan Results</h1>
  {% if results %}
    <table>
      <tr>
        <th>ID</th>
        <th>Source</th>
        <th>Request</th>
        <th>Timestamp</th>
      </tr>
      {% for row in results %}
        <tr>
          <td>{{ row.id }}</td>
          <td>{{ row.source }}</td>
          <td>{{ row.request }}</td>
          <td>{{ row.timestamp }}</td>
        </tr>
      {% endfor %}
    </table>
  {% else %}
    <p>No scan results found.</p>
  {% endif %}
</body>
</html>
"""

def get_results():
    """Fetch the 20 most recent scan records from the database."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, source, request, timestamp
        FROM scan_results
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.route("/")
def index():
    results = get_results()
    return render_template_string(HTML_TEMPLATE, results=results)

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
