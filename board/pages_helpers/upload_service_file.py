def allowed_file(filename):
    allowed_extensions = {'json'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions
