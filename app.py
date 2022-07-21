from flask import Flask

app = Flask(__name__)

@app.route("/", methods = ['GET','POST'])
def index():
    return "start of income prediction project"



if __name__=="__main__":
   app.run()
    