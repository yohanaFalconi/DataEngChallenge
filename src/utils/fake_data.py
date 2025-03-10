import json

fake_data = [{"id": i, "department": f"Department {i}"} for i in range(1, 1002)]

with open("src/utils/fake_departments__.json", "w") as f:
    json.dump(fake_data, f, indent=2)