from flask import render_template, request
from app import app, host, port, user, passwd, db
from app.helpers.database import con_db
import pymysql

# To create a database connection, add the following
# within your view functions:
# con = con_db(host, port, user, passwd, db)


# ROUTING/VIEW FUNCTIONS
@app.route('/')
@app.route('/index')
def index():
    # Renders index.html.
    name = "Me myself"
    
#    return("Hello, {0} How are you!".format(name))

    return render_template('index.html')

@app.route('/pythondbquery', methods=['POST'])
def pythondbquery():
    request.form['query']
    connection = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='public098', db='simpletest')
    myCursor = connection.cursor()
    
    query=request.form['query']
    print query
    print "BEFORE= ", query.strip()
    if myCursor.execute("select drug_name,bestdrug_name,bestdrug_score,worstdrug_name,worstdrug_score from bestWorstDrug where drug_name=\"" + str(query).upper() + "\""):
        print "hello!"
        result = myCursor.fetchall()
    print "RESULT"+ result[0][3]
    
    
    return render_template('pythondbquery.html',  query=query, drug=result[0][0], best=result[0][1],bestscore=result[0][2],worst=result[0][3],worstscore=result[0][4])

@app.route('/home')
def home():
    # Renders home.html.
    return render_template('home.html')

@app.route('/slides')
def about():
    # Renders slides.html.
    return render_template('slides.html')

@app.route('/author')
def contact():
    # Renders author.html.
    return render_template('author.html')

@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500
