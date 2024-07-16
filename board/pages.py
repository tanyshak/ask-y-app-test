import os
from flask import Blueprint, render_template, request, redirect, url_for, current_app, session, Flask
from werkzeug.utils import secure_filename
from flask_session import Session
from board.pages_helpers.upload_service_file import allowed_file
from board.pages_helpers.form_project import bigquery_save_to_storage
from board.pages_helpers.form_snowflake_conn import imort_data_to_snowflake

def allowed_file(filename):
    allowed_extensions = {'json'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

SESSION_TYPE = 'filesystem'
UPLOAD_FOLDER = 'uploads'
FILENAME = secure_filename("service_file.json")

bp = Blueprint("pages", __name__)
app = Flask(__name__)
sess = Session()

app.config['SESSION_TYPE'] = SESSION_TYPE
app.secret_key = os.getenv('SECRET_KEY')

@bp.route('/')
def home():
    return render_template('pages/index.html')

@bp.route('/upload_service_file')
def upload_file():
    return render_template('pages/upload_service_file.html')

@bp.route('/upload_service_file', methods=['POST'])
def upload_service_file():
    if 'file' not in request.files:
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    if file and allowed_file(file.filename):
        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)
        file_path = os.path.join(UPLOAD_FOLDER, FILENAME)
        file.save(file_path)
        return redirect(url_for('pages.form_project'))
    else:
        return redirect(request.url)

@bp.route('/form_project', methods=['POST', 'GET'])
def form_project():
    if request.method == 'POST':
        session['project_name'] = request.form.get('project_name')
        session['dataset_id'] = request.form.get('dataset_id')
        session['table_id'] = request.form.get('table_id')
        session['location'] = request.form.get('location')
        storage_allowed_location = bigquery_save_to_storage(location = session.get('location')
                                                            ,key_path = os.path.join(UPLOAD_FOLDER, FILENAME)
                                                            ,project = session.get('project_name')
                                                            ,dataset_id = session.get('dataset_id')
                                                            ,table_id = session.get('table_id')
                                                            ,file_path = '/sample_app/*.parquet'
                                                            ,bucket= 'data_fisheye_unnest_test_app')
        session['storage_allowed_location'] = storage_allowed_location
        return redirect(url_for('pages.form_snowflake_conn'))
    return render_template('pages/form_project.html')

@bp.route('/form_snowflake_conn', methods=['POST', 'GET'])
def form_snowflake_conn():
    if request.method == 'POST':
        session['user'] = request.form.get('user')
        session['password'] = request.form.get('password')
        session['account'] = request.form.get('account')
        imort_data_to_snowflake(user = session.get('user'),
                                    password = session.get('password'),
                                    account = session.get('account'),
                                    storage_allowed_location = session.get('storage_allowed_location')
                                    )
        return redirect(url_for('pages.form_select_columns'))
    return render_template('pages/form_snowflake_conn.html')


@bp.route('/form_select_columns', methods=['GET', 'POST'])
def form_select_columns():
    if request.method == 'POST':
        session['selected_fields'] = request.form.getlist('fields')
        return redirect(url_for('pages.processing'))
    return render_template('pages/form_select_columns.html')

@bp.route('/processing', methods=['GET', 'POST'])
def processing():
    selected_fields = session.get('selected_fields', [])
    return render_template('pages/processing.html', selected_fields=selected_fields)

app.register_blueprint(bp)
sess.init_app(app)
