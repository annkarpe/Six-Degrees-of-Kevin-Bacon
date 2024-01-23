from flask import Flask, render_template, request, redirect, url_for
from flask import send_from_directory
from algo import read_graph_from_txt, find_degree, graph

app = Flask(__name__)

def load_graph():
    read_graph_from_txt("outp.txt")
    print('Here is the graph:', graph)
    return "Graph loaded successfully!"


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        load_graph()
        user_actor = request.form['userName']
        result, degree = find_degree(user_actor)
        return redirect(url_for('result', actor=user_actor, degree=degree, result=result))
    return render_template('index.html')


@app.route('/result/<actor>/<degree>')
def result(actor, degree):
    steps = find_degree(actor)[0][::-1]
    return render_template('result.html', actor=actor, degree=degree, result=steps)





if __name__ == '__main__':
    app.run(debug=True)
