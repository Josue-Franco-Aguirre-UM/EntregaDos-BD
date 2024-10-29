-- Crear la base de datos
CREATE DATABASE josuecompany;

-- Seleccionar la base de datos
USE josuecompany;

-- Crear la tabla de departamentos
CREATE TABLE departments (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(100),
    place VARCHAR(100),
    department_head INT
);

-- Crear la tabla de empleados
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    department_id INT,
    hire_date DATE,
    salary DECIMAL(10, 2),
    position VARCHAR(50),
    manager_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Crear la tabla de proyectos
CREATE TABLE projects (
    project_id INT AUTO_INCREMENT PRIMARY KEY,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    department_id INT,
    employee_id INT,
    budget DECIMAL(10, 2),
    project_manager INT,
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- Crear la tabla de clientes
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100),
    country VARCHAR(100),
    phone_number VARCHAR(20),
    email VARCHAR(100)
);

-- Crear la tabla de proveedores
CREATE TABLE suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_info VARCHAR(100),
    country VARCHAR(100),
    phone_number VARCHAR(20)
);

-- Crear la tabla de productos
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    supplier_id INT,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Crear la tabla de ventas (o pedidos)
CREATE TABLE sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    sale_date DATE,
    amount DECIMAL(10, 2),
    product_id INT,
    customer_id INT,
    supplier_id INT,
    employee_id INT,
    project_id INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

customers
departments
employees
projects
suppliers
products
sales