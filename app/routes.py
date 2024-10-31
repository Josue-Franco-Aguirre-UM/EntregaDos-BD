from fastapi import APIRouter, HTTPException
from typing import List
from .database import get_db_connection
from . import models
import mysql.connector

router = APIRouter()

@router.post("/departments/", response_model=models.Department, tags=["Departments"])
async def create_department(department: models.DepartmentCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO departments 
                (department_name, place, department_head) 
                VALUES (%s, %s, %s)"""
        values = (department.department_name, department.place, department.department_head)
        cursor.execute(query, values)
        conn.commit()
        
        # Obtener el ID generado
        department_id = cursor.lastrowid
        return {
            "department_id": department_id,
            **department.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/departments/", response_model=List[models.Department], tags=["Departments"])
async def list_departments():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM departments")
        departments = cursor.fetchall()
        return departments
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/departments/bulk/", response_model=List[models.Department], tags=["Departments"])
async def create_departments_bulk(departments: List[models.DepartmentCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO departments 
                (department_name, place, department_head) 
                VALUES (%s, %s, %s)"""
        values = [(d.department_name, d.place, d.department_head) 
                for d in departments]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, dept in enumerate(departments):
            result.append({
                "department_id": first_id + i,
                **dept.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/employees/", response_model=models.Employee, tags=["Employees"])
async def create_employee(employee: models.EmployeeCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO employees 
                (first_name, last_name, email, department_id, 
                hire_date, salary, position, manager_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (employee.first_name, employee.last_name, employee.email,
                employee.department_id, employee.hire_date, employee.salary,
                employee.position, employee.manager_id)
        cursor.execute(query, values)
        conn.commit()
        
        employee_id = cursor.lastrowid
        return {
            "employee_id": employee_id,
            **employee.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/employees/", response_model=List[models.Employee], tags=["Employees"])
async def list_employees():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM employees")
        employees = cursor.fetchall()
        return employees
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/employees/bulk/", response_model=List[models.Employee], tags=["Employees"])
async def create_employees_bulk(employees: List[models.EmployeeCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO employees 
                (first_name, last_name, email, department_id, 
                hire_date, salary, position, manager_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        values = [(e.first_name, e.last_name, e.email, e.department_id,
                e.hire_date, e.salary, e.position, e.manager_id) 
                for e in employees]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, emp in enumerate(employees):
            result.append({
                "employee_id": first_id + i,
                **emp.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/projects/", response_model=models.Project, tags=["Projects"])
async def create_project(project: models.ProjectCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO projects 
                (project_name, start_date, end_date, department_id,
                employee_id, budget, project_manager) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = (project.project_name, project.start_date, project.end_date,
                project.department_id, project.employee_id, project.budget,
                project.project_manager)
        cursor.execute(query, values)
        conn.commit()
        
        project_id = cursor.lastrowid
        return {
            "project_id": project_id,
            **project.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/projects/", response_model=List[models.Project], tags=["Projects"])
async def list_projects():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM projects")
        projects = cursor.fetchall()
        return projects
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/projects/bulk/", response_model=List[models.Project], tags=["Projects"])
async def create_projects_bulk(projects: List[models.ProjectCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO projects 
                (project_name, start_date, end_date, department_id,
                employee_id, budget, project_manager) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = [(p.project_name, p.start_date, p.end_date, p.department_id,
                p.employee_id, p.budget, p.project_manager)
                for p in projects]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, proj in enumerate(projects):
            result.append({
                "project_id": first_id + i,
                **proj.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/customers/", response_model=models.Customer, tags=["Customers"])
async def create_customer(customer: models.CustomerCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO customers 
                (customer_name, country, phone_number, email) 
                VALUES (%s, %s, %s, %s)"""
        values = (customer.customer_name, customer.country,
                customer.phone_number, customer.email)
        cursor.execute(query, values)
        conn.commit()
        
        customer_id = cursor.lastrowid
        return {
            "customer_id": customer_id,
            **customer.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/customers/", response_model=List[models.Customer], tags=["Customers"])
async def list_customers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM customers")
        customers = cursor.fetchall()
        return customers
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/customers/bulk/", response_model=List[models.Customer], tags=["Customers"])
async def create_customers_bulk(customers: List[models.CustomerCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO customers 
                (customer_name, country, phone_number, email) 
                VALUES (%s, %s, %s, %s)"""
        values = [(c.customer_name, c.country, c.phone_number, c.email) 
                for c in customers]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, cust in enumerate(customers):
            result.append({
                "customer_id": first_id + i,
                **cust.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/suppliers/", response_model=models.Supplier, tags=["Suppliers"])
async def create_supplier(supplier: models.SupplierCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO suppliers 
                (supplier_name, contact_info, country, phone_number) 
                VALUES (%s, %s, %s, %s)"""
        values = (supplier.supplier_name, supplier.contact_info,
                supplier.country, supplier.phone_number)
        cursor.execute(query, values)
        conn.commit()
        
        supplier_id = cursor.lastrowid
        return {
            "supplier_id": supplier_id,
            **supplier.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/suppliers/", response_model=List[models.Supplier], tags=["Suppliers"])
async def list_suppliers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM suppliers")
        suppliers = cursor.fetchall()
        return suppliers
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/suppliers/bulk/", response_model=List[models.Supplier], tags=["Suppliers"])
async def create_suppliers_bulk(suppliers: List[models.SupplierCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO suppliers 
                (supplier_name, contact_info, country, phone_number) 
                VALUES (%s, %s, %s, %s)"""
        values = [(s.supplier_name, s.contact_info, s.country, s.phone_number) 
                for s in suppliers]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, supp in enumerate(suppliers):
            result.append({
                "supplier_id": first_id + i,
                **supp.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/products/", response_model=models.Product, tags=["Products"])
async def create_product(product: models.ProductCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO products 
                (product_name, supplier_id) 
                VALUES (%s, %s)"""
        values = (product.product_name, product.supplier_id)
        cursor.execute(query, values)
        conn.commit()
        
        product_id = cursor.lastrowid
        return {
            "product_id": product_id,
            **product.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/products/", response_model=List[models.Product], tags=["Products"])
async def list_products():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM products")
        products = cursor.fetchall()
        return products
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/products/bulk/", response_model=List[models.Product], tags=["Products"])
async def create_products_bulk(products: List[models.ProductCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO products 
                (product_name, supplier_id) 
                VALUES (%s, %s)"""
        values = [(p.product_name, p.supplier_id) for p in products]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, prod in enumerate(products):
            result.append({
                "product_id": first_id + i,
                **prod.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/sales/", response_model=models.Sale, tags=["Sales"])
async def create_sale(sale: models.SaleCreate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """INSERT INTO sales 
                (sale_date, amount, product_id, customer_id,
                supplier_id, employee_id, project_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = (sale.sale_date, sale.amount, sale.product_id,
                sale.customer_id, sale.supplier_id, sale.employee_id, sale.project_id)
        cursor.execute(query, values)
        conn.commit()
        
        sale_id = cursor.lastrowid
        return {
            "sale_id": sale_id,
            **sale.dict()
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.get("/sales/", response_model=List[models.Sale], tags=["Sales"])
async def list_sales():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sales")
        sales = cursor.fetchall()
        return sales
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@router.post("/sales/bulk/", response_model=List[models.Sale], tags=["Sales"])
async def create_sales_bulk(sales: List[models.SaleCreate]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """INSERT INTO sales 
                (sale_date, amount, product_id, customer_id,
                supplier_id, employee_id, project_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = [(s.sale_date, s.amount, s.product_id, s.customer_id,
                s.supplier_id, s.employee_id, s.project_id)
                for s in sales]
        cursor.executemany(query, values)
        conn.commit()
        
        first_id = cursor.lastrowid
        result = []
        for i, sale in enumerate(sales):
            result.append({
                "sale_id": first_id + i,
                **sale.dict()
            })
        return result
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()