
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)


from algo import read_graph_from_txt, find_degree, graph


#@app.route('/load-graph')
def load_graph():
    read_graph_from_txt("outp.txt")
    print('Here is the graph:', graph)
    return "Graph loaded successfully!"

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        print("post is successful") 
        load_graph()
        user_actor = request.form['userName']
        print('user_actor', user_actor)
        degree = find_degree(user_actor)
        return redirect(url_for('result', actor=user_actor, degree=degree))
    return render_template('index.html')

@app.route('/result/<actor>/<degree>')
def result(actor, degree):
    return render_template('result.html', actor=actor, degree=degree)

if __name__ == '__main__':
    app.run(debug=True)
