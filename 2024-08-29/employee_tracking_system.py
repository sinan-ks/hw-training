import json
from datetime import datetime
from pymongo import MongoClient

class EmployeeTaskTracker:
    client = MongoClient('localhost', 27017) 
    db = client['employee_tracking']  
    main_task_list = []

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        self.tasks = []
        self.login_time = None
        self.logout_time = None

    def log_in(self):
        self.login_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        print(f'{self.emp_name} logged in at {self.login_time}')

    def add_task(self, task_title, task_description):
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
        EmployeeTaskTracker.main_task_list.append(self.tasks[-1])
        print(f'Ended task: {self.tasks[-1]["task_title"]} at {self.tasks[-1]["end_time"]}')

    def log_out(self):
        self.logout_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        print(f'{self.emp_name} logged out at {self.logout_time}')
        self._create_daily_json_file()
        self._save_to_mongodb()

    def _create_daily_json_file(self):
        file_name = f'{self.emp_name}_{datetime.now().strftime("%Y-%m-%d")}.json'
        data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }
        with open(file_name, 'w') as f:
            json.dump(data, f, indent=4)
        print(f'Task details saved in {file_name}')

    def _save_to_mongodb(self):
        collection_name = f"{self.emp_name.replace(' ', '_')}_{self.emp_id}"
        collection = self.db[collection_name]  # Create a collection with the employee's name and ID

        data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }
        collection.insert_one(data)  # Use the dynamically created collection
        print(f'Task details saved in MongoDB for {self.emp_name}')


if __name__ == "__main__":

    employee1 = EmployeeTaskTracker("Sinan", 1)
    employee1.log_in()
    employee1.add_task("Task-1 Data Scraping", "Completed the task and saved the data")
    employee1.end_task(success=True)
    employee1.add_task("Task-2 Employee Tracker", "Doing the task")
    employee1.end_task(success=False)
    employee1.add_task("Task-3 Verifying Task-2", "Checking with inputs")
    employee1.end_task(success=True)
    employee1.log_out()
