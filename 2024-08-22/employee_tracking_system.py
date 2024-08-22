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

    def start_task(self, task_title, task_description):
        task = {
            "task_title": task_title,
            "task_description": task_description,
            "start_time": datetime.now().strftime('%Y-%m-%d %H:%M'),
            "end_time": None,
            "task_success": False
        }
        self.tasks.append(task)
        print(f'Started task: {task_title} at {task["start_time"]}')

    def end_task(self, success=True):
        if not self.tasks:
            print("No task to end.")
            return

        self.tasks[-1]["end_time"] = datetime.now().strftime('%Y-%m-%d %H:%M')
        self.tasks[-1]["task_success"] = success
        EmployeeTaskTracker.employee_task_list.append(self.tasks[-1])
        print(f'Ended task: {self.tasks[-1]["task_title"]} at {self.tasks[-1]["end_time"]}')