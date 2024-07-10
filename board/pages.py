import os
from flask import Blueprint ,render_template, request, redirect, url_for, current_app, session
from werkzeug.utils import secure_filename


bp = Blueprint("pages", __name__)

def allowed_file(filename):
    allowed_extensions = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

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
        filename = secure_filename(file.filename)
        file.save(os.path.join('uploads', filename))
        return redirect(url_for('pages.form_project'))
    else:
        return redirect(request.url)


@bp.route('/form_project', methods=['POST', 'GET'])
def form_project():
    project_name = request.form.get('project_name')
    dataset_id = request.form.get('dataset_id')
    table_id = request.form.get('table_id')
    return render_template('pages/form_select_columns.html')

@bp.route('/form_select_columns', methods=['GET', 'POST'])
def form_select_columns():
    if request.method == 'POST':
        selected_fields = request.form.getlist('fields')
        return redirect(url_for('pages.processing', selected_fields=selected_fields))
    return render_template('pages/form_select_columns.html')

@bp.route('/processing', methods=['GET', 'POST'])
def processing():
    selected_fields = session.get('selected_fields', [])
    return render_template('pages/processing.html', selected_fields=selected_fields)

def app():
    bp.run(debug=False)

if __name__ == '__main__':
    bp.run(debug=True)
