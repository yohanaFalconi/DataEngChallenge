import json
from datetime import datetime, timedelta
import random


# fake_data_department = [{"id": i, "department": f"Department {i}"} for i in range(1, 1002)]
fake_hired_employee= []

departments = list(range(1, 11))  
jobs = list(range(1, 6))
base_date = datetime(2020, 1, 1)

for i in range(1, 1002):
    record = {
        "id": i,
        "name": f"Employee {i}",
        "datetime": (base_date + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d"),
        "department_id": random.choice(departments),
        "job_id": random.choice(jobs),
    }
    fake_hired_employee.append(record)


with open("src/utils/fake_hired_employees.json", "w") as f:
    json.dump(fake_hired_employee, f, indent=2)