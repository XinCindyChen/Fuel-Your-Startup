from flask import render_template, request
from app import app
import pymysql as mdb
from alg import recommender_model

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/input')
def company_input():
  return render_template("index.html")


@app.route('/output')
def investor_output():
  #pull 'ID' from input field and store it
  company_name = request.args.get('company-name')
  
    
  '''
  with db:
    cur = db.cursor()
    #just select the city from the world_innodb that the user inputs
    cur.execute("SELECT Name, CountryCode,  Population FROM City WHERE Name='%s';" % city)
    query_results = cur.fetchall()

  cities = []
  for result in query_results:
    cities.append(dict(name=result[0], country=result[1], population=result[2]))
  '''
 #call a function from a_Model package. note we are only pulling one result in the query
  #pop_input = cities[0]['population']
  model_results = recommender_model(company_name)
  if model_results=='error1':
    return render_template('error.html')
  else:
    (predicted_investors, investor_list, network_filename, company_origin_name, company_location, company_categories, funding_round) = model_results
  
    return render_template("output.html", predicted_investors = predicted_investors, investor_list = investor_list, network_filename = network_filename, company_name = company_origin_name, company_location=company_location, company_categories=company_categories, funding_round=funding_round)

@app.route('/test')
def test_func():
  return render_template('test.html')










