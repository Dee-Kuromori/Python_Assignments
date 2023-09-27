"""
Join two CSV files based on a common key,
such as joining a list of customers and
their orders based on customer IDs.
"""

import pandas as pd

employee_df = pd.read_csv(r"C:\Users\Consultant\Downloads\Employee.csv")


salary_employee_df = employee_df.copy()[["EmployeeID","Salary"]]
salary_employee_df.to_csv(r"C:\Users\Consultant\Documents\Hadoop\Assignments\salary_employees.csv")

name_employee_df = employee_df.copy()[["EmployeeID","Name"]]
name_employee_df.to_csv(r"C:\Users\Consultant\Documents\Hadoop\Assignments\name_employees.csv")
# print(employee_df[["EmployeeID","Salary"]])
# print(employee_df)
# # print(employee_df)
# finance_employee_df = employee_df.copy()[employee_df["Department"] == "Finance"]
# print(finance_employee_df)
# # finance_employee_df.to_csv(r"C:\Users\Consultant\Documents\Hadoop\Assgnments\finance_employees.csv")
#
# engineering_employee_df = employee_df[employee_df["Department"] == "Engineering"]
# print(engineering_employee_df)
# engineering_employee_df.to_csv(r"C:\Users\Consultant\Documents\Hadoop\Assgnments\engineering_employees.csv")
# print(employee_df.filter(employee_df["Department"] == "HR"))

employee_df = pd.read_csv(r"C:\Users\Consultant\Downloads\Employee.csv",index_col="EmployeeID")
print(employee_df)


