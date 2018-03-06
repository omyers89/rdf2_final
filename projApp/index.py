from flask import *
import time
import sys
import os



#sys.path.append(os.path.dirname(__file__)+'/../my_code/')
#from feature_miner import FeatureMiner, DBPEDIA_URL_UP
from my_code.MLStaff_new import create_training_data, get_class_with_prob
from my_code.getTruth import get_current, check_const

app = Flask(__name__)

login_state = False
DEBUG = True
# @app.route('/index/')
@app.route('/answer/<span>/<proba>/<relevant>/<value>')
def answer(span=None, proba=None, relevant=None, value=None):
    return render_template('answer.html', is_temporal_t=span,proba=proba,  relevant_t=relevant, value_t=value)

@app.route('/hello')
def hello_world():
    return render_template('query.html', error=None)

@app.route('/properties/<prop>')
def show_property_features(prop):
    # get features for property and display them
    return 'Hello,%s' % prop

# @app.route('/login', methods=['POST', 'GET'])
# def login():
#     error = None
#     if request.method == 'POST':
#         if valid_login(request.form['username'],
#             request.form['password']):
#             return log_the_user_in(request.form['username'])
#     else:
#         error = 'Invalid username/password'
#         # the my_code below is executed if the request method
#         # was GET or the credentials were invalid
#     return render_template('query.html', error=error)


@app.route('/query', methods=['GET', 'POST'])
def query():
    error = None
    if request.method == 'POST':
        subj = request.form['subj_uri']
        prop = request.form['prop_uri']
        obj_uri = request.form['obj_uri']

        if DEBUG:
            if subj == "":
                subj = "http://dbpedia.org/resource/Donald_Trump"
            if prop == "":
                prop = "http://dbpedia.org/property/spouse"
            if obj_uri == "":
                obj_uri = "http://dbpedia.org/resource/Ivana_Trump"

        print ("in query")
        session['logged_in'] = True
        (span, prob, relevant, value) = get_results(subj, prop, obj_uri)
        # if request.form['subj_uri'] != 'omri':
        #     error = 'Invalid username'
        # elif request.form['password'] != '1234':
        #     error = 'Invalid password'
        # else:
        #
        #     flash('You were logged in')

        return redirect(url_for('answer', span=span, proba=prob, relevant=relevant, value=value))
    else:
        return render_template('query.html', error=error)


@app.route('/')
def index():
    return render_template('query.html', error=None)


@app.route('/page')
def get_page():
    return url_for('templates/progress.html')

def get_results(subj, prop, obj_uri):
    isCurrent = True
    cur_obj = ""
    cur_year = ""
    curr_val = "ERROR"
    res = False
    prob = 1
    try:
        # check if temporal or ethernal: Temporal = true Ethernal=false ,
        # prop =http://dbpedia.org/ontology/birthPlace
        (res,prob) = get_class_with_prob(prop, False, 75)
        #IsCurrent: true/false

        if res: # the property is temporal
            # check if is current
            isCurrent, cur_obj, cur_year = get_current(subj,prop, obj_uri)

            if not isCurrent:
                # if not current get the current
                curr_val = cur_obj + " from: " + cur_year
        else:
            isCurrent, cur_obj, cur_year = check_const(subj, prop, obj_uri)
            if not isCurrent:
                # if not current get the current
                curr_val = cur_obj
    except Exception as e:
        print e
    return res, prob, isCurrent, curr_val

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('query'))

def valid_login(uname, upass):
    if uname == "omri" and upass == "1234":
        return True
    return False

def log_the_user_in(uname):
    return redirect(url_for('hello', name=uname))

@app.route('/progress')
def progress():
    #FM = FeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    #features = FM.get_fetures_for_prop(quick=True, prop_uri=prop_uri)
    def generate():
        x = 0
        while x < 100:
            print (x)
            x = x + 10
            time.sleep(0.2)
            yield "data:" + str(x) + "\n\n"
    return Response(generate(), mimetype='text/event-stream')

app.secret_key = 'Dev_Key'

if __name__ == '__main__':
    app.run()

    '''
    demo query:
    
        http://dbpedia.org/resource/Donald_Trump
        http://dbpedia.org/property/spouse
        http://dbpedia.org/resource/Ivana_Trump
        
        http://dbpedia.org/ontology/birthPlace
        
    '''