from flask import Flask, request, render_template_string
from bi_agent import answer_founder_question

app = Flask(__name__)

# Minimal HTML template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Founder BI AI Agent</title>
</head>
<body>
    <h1>Founder BI AI Agent</h1>
    <form method="post">
        <label for="question">Ask a question:</label><br>
        <input type="text" id="question" name="question" size="60" value="{{ question|default('') }}"><br><br>
        <input type="submit" value="Submit">
    </form>

    {% if answer %}
        <h2>Answer:</h2>
        <pre>{{ answer }}</pre>
    {% endif %}
</body>
</html>
"""

@app.route("/", methods=["GET", "POST"])
def home():
    question = ""
    answer = None
    if request.method == "POST":
        question = request.form.get("question", "")
        if question:
            try:
                answer = answer_founder_question(question)
            except Exception as e:
                answer = f"Error: {e}"
    return render_template_string(HTML_TEMPLATE, question=question, answer=answer)

if __name__ == "__main__":
    app.run(debug=True)