from fastapi import APIRouter, HTTPException
from typing import List
from .database import get_db_connection
from . import models
import mysql.connector
from datetime import date
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
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM departments")
        existing_departments = cursor.fetchall()
        print("\nExisting departments in database:", existing_departments)
        
        print("\nEmployees to insert:")
        for emp in employees:
            print(f"- {emp.first_name} {emp.last_name} (Dept ID: {emp.department_id})")
        
        department_ids = {emp.department_id for emp in employees}
        print("\nDepartment IDs needed:", department_ids)
        
        placeholders = ','.join(['%s'] * len(department_ids))
        cursor.execute(
            f"SELECT department_id FROM departments WHERE department_id IN ({placeholders})", 
            tuple(department_ids)
        )
        existing_dept_ids = {row['department_id'] for row in cursor.fetchall()}
        print("Department IDs found:", existing_dept_ids)
        
        missing_dept_ids = department_ids - existing_dept_ids
        if missing_dept_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Departments with IDs {missing_dept_ids} do not exist. "
                    f"Please create these departments first. "
                    f"Existing department IDs are: {existing_dept_ids}"
            )
        
        query = """INSERT INTO employees 
                (first_name, last_name, email, department_id, 
                hire_date, salary, position, manager_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        
        values = [(e.first_name, e.last_name, e.email, e.department_id,
                e.hire_date, e.salary, e.position, e.manager_id) 
                for e in employees]
        
        result = []
        for i, value in enumerate(values):
            try:
                print(f"\nTrying to insert employee {i+1}:", value)
                cursor.execute(query, value)
                employee_id = cursor.lastrowid
                result.append({
                    "employee_id": employee_id,
                    **employees[i].dict()
                })
                print(f"Successfully inserted employee {i+1}")
            except mysql.connector.Error as err:
                print(f"Error inserting employee {i+1}: {err}")
                conn.rollback()
                raise HTTPException(
                    status_code=400,
                    detail=f"Error inserting employee {employees[i].first_name} {employees[i].last_name}: {str(err)}"
                )
        
        conn.commit()
        return result
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        raise HTTPException(
            status_code=400,
            detail=f"Database error: {str(err)}"
        )
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
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM suppliers")
        existing_suppliers = cursor.fetchall()
        print("\nExisting suppliers in database:", existing_suppliers)
        
        print("\nProducts to insert:")
        for p in products:
            print(f"Product: {p.dict()}")
        
        supplier_ids = {p.supplier_id for p in products}
        print("\nSupplier IDs needed:", supplier_ids)
        
        if not existing_suppliers:
            raise HTTPException(
                status_code=400,
                detail="No suppliers found in database. Please create suppliers first."
            )
        
        existing_supplier_ids = {s['supplier_id'] for s in existing_suppliers}
        print("Existing supplier IDs:", existing_supplier_ids)
        
        missing_ids = supplier_ids - existing_supplier_ids
        if missing_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Suppliers with IDs {missing_ids} do not exist. Available supplier IDs are: {existing_supplier_ids}"
            )
        
        result = []
        for product in products:
            try:
                query = """INSERT INTO products (product_name, supplier_id) VALUES (%s, %s)"""
                values = (product.product_name, product.supplier_id)
                print(f"\nInserting product: {values}")
                
                cursor.execute(query, values)
                product_id = cursor.lastrowid
                
                result.append({
                    "product_id": product_id,
                    **product.dict()
                })
            except mysql.connector.Error as err:
                print(f"Error inserting product: {err}")
                conn.rollback()
                raise HTTPException(
                    status_code=400,
                    detail=f"Error inserting product {product.product_name}: {str(err)}"
                )
        
        conn.commit()
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
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


@router.get("/Query1/", tags=["Query1: Average Salary by Department"])
def get_avg_salary_by_dept():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                d.department_name,
                AVG(e.salary) as average_salary
            FROM departments d
            LEFT JOIN employees e ON d.department_id = e.department_id
            GROUP BY d.department_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query2/", tags=["Query2: Min Sales by Product"])
def get_min_sales():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                p.product_name,
                MIN(s.amount) as minimum_sale
            FROM products p
            INNER JOIN sales s ON p.product_id = s.product_id
            GROUP BY p.product_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query3/", tags=["Query3: Max Customer Purchases"])
def get_max_customer_purchases():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                c.customer_name,
                MAX(s.amount) as largest_purchase
            FROM sales s
            RIGHT JOIN customers c ON s.customer_id = c.customer_id
            GROUP BY c.customer_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query4/", tags=["Query4: Employee Count"])
def get_employee_count():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                d.department_name,
                COUNT(e.employee_id) as employee_count
            FROM departments d
            LEFT JOIN employees e ON d.department_id = e.department_id
            GROUP BY d.department_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query5/", tags=["Query5: Avg Sales per Employee"])
def get_avg_sales_employee():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                CONCAT(e.first_name, ' ', e.last_name) as employee_name,
                AVG(s.amount) as average_sale
            FROM employees e
            INNER JOIN sales s ON e.employee_id = s.employee_id
            GROUP BY employee_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query6/", tags=["Query6: Products per Supplier"])
def get_supplier_products():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                s.supplier_name,
                COUNT(p.product_id) as total_products
            FROM products p
            RIGHT JOIN suppliers s ON p.supplier_id = s.supplier_id
            GROUP BY s.supplier_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query7/", tags=["Query7: Max Budget by Department"])
def get_max_budget():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                d.department_name,
                MAX(p.budget) as highest_budget
            FROM departments d
            LEFT JOIN projects p ON d.department_id = p.department_id
            GROUP BY d.department_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query8/", tags=["Query8: Min Sales by Country"])
def get_min_sales_country():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                c.country,
                MIN(s.amount) as minimum_sale
            FROM customers c
            INNER JOIN sales s ON c.customer_id = s.customer_id
            GROUP BY c.country
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query9/", tags=["Query9: Avg Products per Supplier"])
def get_avg_products_supplier():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                s.country,
                AVG(p.product_id) as avg_products
            FROM products p
            RIGHT JOIN suppliers s ON p.supplier_id = s.supplier_id
            GROUP BY s.country
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query10/", tags=["Query10: Sales by Project"])
def get_project_sales():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                p.project_name,
                COUNT(s.sale_id) as total_sales
            FROM projects p
            LEFT JOIN sales s ON p.project_id = s.project_id
            GROUP BY p.project_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query11/", tags=["Query11: Max Salary by Position"])
def get_max_salary_position():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                e.position,
                MAX(e.salary) as highest_salary
            FROM employees e
            INNER JOIN departments d ON e.department_id = d.department_id
            GROUP BY e.position
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query12/", tags=["Query12: Avg Sales by Country"])
def get_avg_sales_country():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                c.country,
                AVG(s.amount) as average_sale
            FROM sales s
            RIGHT JOIN customers c ON s.customer_id = c.customer_id
            GROUP BY c.country
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query13/", tags=["Query13: Min Employees per Project"])
def get_min_employees_project():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                p.project_name,
                MIN(e.employee_id) as min_employees
            FROM projects p
            LEFT JOIN employees e ON p.department_id = e.department_id
            GROUP BY p.project_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query14/", tags=["Query14: Sales by Supplier"])
def get_supplier_sales():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                s.supplier_name,
                COUNT(sa.sale_id) as total_sales
            FROM suppliers s
            INNER JOIN products p ON s.supplier_id = p.supplier_id
            INNER JOIN sales sa ON p.product_id = sa.product_id
            GROUP BY s.supplier_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.get("/Query15/", tags=["Query15: Avg Budget by Department"])
def get_avg_budget_dept():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                d.department_name,
                AVG(p.budget) as average_budget
            FROM projects p
            RIGHT JOIN departments d ON p.department_id = d.department_id
            GROUP BY d.department_name
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()