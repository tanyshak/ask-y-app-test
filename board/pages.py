import os
from flask import Blueprint, render_template, request, redirect, url_for, current_app, session, Flask
from werkzeug.utils import secure_filename
from flask_session import Session
from board.pages_helpers.upload_service_file import allowed_file
from board.pages_helpers.form_project import bigquery_save_to_storage
from board.pages_helpers.bigquery import bigquery_get_date_range, generate_gcloud_commands
from board.pages_helpers.form_snowflake_conn import imort_data_to_snowflake
from board.pages_helpers.snowflake_unnest import unnest_snowflake_table, create_conn

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
conn = None

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

@bp.route('/instructions', methods=['GET', 'POST'])
def instructions():
    commands = None
    if request.method == 'POST':
        project_id = request.form['project_id']
        commands = generate_gcloud_commands(project_id)
    return render_template('pages/instructions.html', commands=commands)

@bp.route('/form_project', methods=['POST', 'GET'])
def form_project():
    if request.method == 'POST':
        session['project_id'] = request.form.get('project_id')
        session['dataset_id'] = request.form.get('dataset_id')
        very_start_date, very_end_date = bigquery_get_date_range(key_path = os.path.join(UPLOAD_FOLDER, FILENAME)
                                                                ,project_id = session.get('project_id')
                                                                ,dataset_id = session.get('dataset_id'))
        session['very_start_date'] = very_start_date
        session['very_end_date'] = very_end_date
        return redirect(url_for('pages.form_date_range'))
    return render_template('pages/form_project.html')

@bp.route('/form_date_range', methods=['POST', 'GET'])
def form_date_range():
    if request.method == 'POST':
        session['start_date'] = request.form.get('start_date')
        session['end_date'] = request.form.get('end_date')
        storage_allowed_location, table_id = bigquery_save_to_storage(key_path = os.path.join(UPLOAD_FOLDER, FILENAME)
                                                            ,project_id = session.get('project_id')
                                                            ,dataset_id = session.get('dataset_id')
                                                            ,start_date = session.get('start_date')
                                                            ,end_date = session.get('end_date')
                                                            ,file_path = '/sample_app/*.parquet'
                                                            ,bucket = f"data_{session.get('project_id')}_nested_test_app")

        session['storage_allowed_location'] = storage_allowed_location
        session['table_id'] = table_id
        return redirect(url_for('pages.form_snowflake_conn'))
    very_start_date = session.get('very_start_date', 'None')
    very_end_date = session.get('very_end_date', 'None')
    return render_template('pages/form_date_range.html', very_start_date=very_start_date, very_end_date=very_end_date)

@bp.route('/form_snowflake_conn', methods=['POST', 'GET'])
def form_snowflake_conn():
    global conn
    if request.method == 'POST':
        user = request.form.get('user')
        password = request.form.get('password')
        account = request.form.get('account')
        conn = create_conn(user = user, password = password, account = account)
        project_id = session.get('project_id')
        dataset_id = session.get('dataset_id')
        table_id = session.get('table_id')
        table_name = f'{dataset_id}_{table_id}'
        imort_data_to_snowflake(conn = conn
                                ,storage_allowed_location = session.get('storage_allowed_location')
                                ,table_name = table_name
                                ,si_name = f'si_snowflake_{table_name}'
                                ,db_name = project_id
                                ,stage_name = f'stage_{table_name}'
                                ,file_format_name = f'{table_name}_format')
        session['snowflake_nested_table_name'] = table_name
        return redirect(url_for('pages.snowflake_unnest'))
    return render_template('pages/form_snowflake_conn.html')

@bp.route('/snowflake_unnest', methods=['POST', 'GET'])
def snowflake_unnest():
    global conn
    if request.method == 'POST':
        logger.info('Received POST request to unnest data')
        try:
            source_table_name = session.get('snowflake_nested_table_name')
            _, all_columns = unnest_snowflake_table(conn = conn,
                                   target_table_name = f"{source_table_name}_unnested",
                                   source_table_name = source_table_name)
            session['all_columns'] = all_columns
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!session['all_columns'] = all_columns")
            print(all_columns)
            logger.info('Unnesting logic executed successfully')
        except Exception as e:
            logger.error(f'Error during unnesting: {e}')
            raise
        return redirect(url_for('pages.form_select_columns'))
    return render_template('pages/snowflake_unnest.html')

@bp.route('/form_select_columns', methods=['GET', 'POST'])
def form_select_columns():
    if request.method == 'POST':
        session['selected_fields'] = request.form.getlist('fields')
        return redirect(url_for('pages.processing'))
    all_columns = session.get('all_columns', [])
    return render_template('pages/form_select_columns.html', all_columns=all_columns)

@bp.route('/processing', methods=['GET', 'POST'])
def processing():
    selected_fields = session.get('selected_fields', [])
    return render_template('pages/processing.html', selected_fields=selected_fields)

app.register_blueprint(bp)
sess.init_app(app)
