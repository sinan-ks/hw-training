import json
from datetime import datetime

class EmployeeTaskTracker:
    employee_task_list = []

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        self.tasks = []
        self.login_time = None
        self.logout_time = None

    def log_in(self):
        self.login_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        print(f'{self.emp_name} logged in at {self.login_time}')