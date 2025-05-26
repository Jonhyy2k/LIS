from flask import Flask, request, render_template, redirect, url_for, send_file, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
import os
from datetime import datetime
import shutil
# from Inputs_Cur import populate_valuation_model  # Import your function # This line will be removed
from dcf_automator import DCFModel 

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Database configuration
DB_CONFIG = {
    'dbname': 'lisquant_db',
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username

@login_manager.user_loader
def load_user(user_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
    user_data = cur.fetchone() 
    cur.close()
    conn.close()
    if user_data:
        return User(user_data[0], user_data[1])
    return None

# Database connection helper
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, username, password FROM users WHERE username = %s", (username,))
        user_data = cur.fetchone() 
        cur.close()
        conn.close()
        if user_data and check_password_hash(user_data[2], password):
            login_user(User(user_data[0], user_data[1]))
            return redirect(url_for('dashboard'))
        flash('Invalid username or password')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            flash('Username already exists')
            cur.close()
            conn.close()
            return render_template('register.html')
        hashed_password = generate_password_hash(password)
        cur.execute("INSERT INTO users (username, password) VALUES (%s, %s) RETURNING id",
                    (username, hashed_password))
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        login_user(User(user_id, username))
        return redirect(url_for('dashboard'))
    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/dashboard')
@login_required
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, ticker, filename, created_at FROM analyses WHERE user_id = %s ORDER BY created_at DESC",
                (current_user.id,))
    analyses = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('dashboard.html', analyses=analyses)

@app.route('/analyze', methods=['POST'])
@login_required
def analyze():
    ticker = request.form['ticker'].strip().upper()
    if not ticker:
        flash('Ticker symbol cannot be empty')
        return redirect(url_for('dashboard'))

    template_path = "LIS_Valuation_Empty.xlsx" 
    output_dir = "user_files"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"{ticker}_Valuation_Model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    try:
        # Instantiate the DCFModel
        model = DCFModel(ticker) 

        # Run the DCF analysis steps
        model.download_data() 
        model.calculate_historical_metrics()
        model.generate_projections()
        model.calculate_dcf() 

        # Populate the Excel template with the model's data
        model.populate_excel_template(template_path=template_path, output_path=output_path)
        
        # Ensure file was actually created by populate_excel_template
        if not os.path.exists(output_path):
             flash(f"Excel file generation failed for {ticker} after analysis.")
             return redirect(url_for('dashboard'))

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO analyses (user_id, ticker, filename, created_at) VALUES (%s, %s, %s, %s)",
                    (current_user.id, ticker, output_filename, datetime.now()))
        conn.commit()
        cur.close()
        conn.close()
        return send_file(output_path, as_attachment=True)
    except Exception as e:
        flash(f"Error processing analysis: {str(e)}")
        import traceback
        traceback.print_exc() 
        return redirect(url_for('dashboard'))

@app.route('/download/<analysis_id>')
@login_required
def download(analysis_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT filename FROM analyses WHERE id = %s AND user_id = %s",
                (analysis_id, current_user.id))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        file_path = os.path.join("user_files", result[0])
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        flash('File not found')
    else:
        flash('Analysis not found or access denied')
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    if not os.path.exists("user_files"):
        os.makedirs("user_files")
    app.run(host='0.0.0.0', port=5000, debug=True)
