from flask import Flask
import script

app = Flask(__name__)

@app.route('/')
def hello_world():
    return script.main()

if __name__ == '__main__':
    app.run()
