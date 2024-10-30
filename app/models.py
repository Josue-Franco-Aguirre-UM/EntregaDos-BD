from pydantic import BaseModel, Field
from datetime import date
from typing import Optional
from decimal import Decimal

# Department models
class DepartmentCreate(BaseModel):
    department_name: str = Field(..., description="Nombre del departamento")
    place: str = Field(..., description="Lugar del departamento")
    department_head: int = Field(..., description="ID del jefe de departamento")

class Department(BaseModel):
    department_id: int = Field(..., description="ID del departamento")
    department_name: str = Field(..., description="Nombre del departamento")
    place: str = Field(..., description="Lugar del departamento")
    department_head: int = Field(..., description="ID del jefe de departamento")

# Employee models
class EmployeeCreate(BaseModel):
    first_name: str = Field(..., description="Nombre del empleado")
    last_name: str = Field(..., description="Apellido del empleado")
    email: str = Field(..., description="Email del empleado")
    department_id: int = Field(..., description="ID del departamento")
    hire_date: date = Field(..., description="Fecha de contratación")
    salary: Decimal = Field(..., description="Salario del empleado")
    position: str = Field(..., description="Cargo del empleado")
    manager_id: Optional[int] = Field(None, description="ID del manager")

class Employee(BaseModel):
    employee_id: int = Field(..., description="ID del empleado")
    first_name: str = Field(..., description="Nombre del empleado")
    last_name: str = Field(..., description="Apellido del empleado")
    email: str = Field(..., description="Email del empleado")
    department_id: int = Field(..., description="ID del departamento")
    hire_date: date = Field(..., description="Fecha de contratación")
    salary: Decimal = Field(..., description="Salario del empleado")
    position: str = Field(..., description="Cargo del empleado")
    manager_id: Optional[int] = Field(None, description="ID del manager")

# Project models
class ProjectCreate(BaseModel):
    project_name: str = Field(..., description="Nombre del proyecto")
    start_date: date = Field(..., description="Fecha de inicio")
    end_date: date = Field(..., description="Fecha de finalización")
    department_id: int = Field(..., description="ID del departamento")
    employee_id: int = Field(..., description="ID del empleado")
    budget: Decimal = Field(..., description="Presupuesto del proyecto")
    project_manager: int = Field(..., description="ID del gerente del proyecto")

class Project(BaseModel):
    project_id: int = Field(..., description="ID del proyecto")
    project_name: str = Field(..., description="Nombre del proyecto")
    start_date: date = Field(..., description="Fecha de inicio")
    end_date: date = Field(..., description="Fecha de finalización")
    department_id: int = Field(..., description="ID del departamento")
    employee_id: int = Field(..., description="ID del empleado")
    budget: Decimal = Field(..., description="Presupuesto del proyecto")
    project_manager: int = Field(..., description="ID del gerente del proyecto")

# Customer models
class CustomerCreate(BaseModel):
    customer_name: str = Field(..., description="Nombre del cliente")
    country: str = Field(..., description="País del cliente")
    phone_number: str = Field(..., description="Número de teléfono")
    email: str = Field(..., description="Email del cliente")

class Customer(BaseModel):
    customer_id: int = Field(..., description="ID del cliente")
    customer_name: str = Field(..., description="Nombre del cliente")
    country: str = Field(..., description="País del cliente")
    phone_number: str = Field(..., description="Número de teléfono")
    email: str = Field(..., description="Email del cliente")

# Supplier models
class SupplierCreate(BaseModel):
    supplier_name: str = Field(..., description="Nombre del proveedor")
    contact_info: str = Field(..., description="Información de contacto")
    country: str = Field(..., description="País del proveedor")
    phone_number: str = Field(..., description="Número de teléfono")

class Supplier(BaseModel):
    supplier_id: int = Field(..., description="ID del proveedor")
    supplier_name: str = Field(..., description="Nombre del proveedor")
    contact_info: str = Field(..., description="Información de contacto")
    country: str = Field(..., description="País del proveedor")
    phone_number: str = Field(..., description="Número de teléfono")

# Product models
class ProductCreate(BaseModel):
    product_name: str = Field(..., description="Nombre del producto")
    supplier_id: int = Field(..., description="ID del proveedor")

class Product(BaseModel):
    product_id: int = Field(..., description="ID del producto")
    product_name: str = Field(..., description="Nombre del producto")
    supplier_id: int = Field(..., description="ID del proveedor")


class SaleCreate(BaseModel):
    sale_date: date = Field(..., description="Fecha de venta")
    amount: Decimal = Field(..., description="Monto de la venta")
    product_id: int = Field(..., description="ID del producto")
    customer_id: int = Field(..., description="ID del cliente")
    supplier_id: int = Field(..., description="ID del proveedor")
    employee_id: int = Field(..., description="ID del empleado")
    project_id: Optional[int] = Field(None, description="ID del proyecto")

class Sale(BaseModel):
    sale_id: int = Field(..., description="ID de la venta")
    sale_date: date = Field(..., description="Fecha de venta")
    amount: Decimal = Field(..., description="Monto de la venta")
    product_id: int = Field(..., description="ID del producto")
    customer_id: int = Field(..., description="ID del cliente")
    supplier_id: int = Field(..., description="ID del proveedor")
    employee_id: int = Field(..., description="ID del empleado")
    project_id: Optional[int] = Field(None, description="ID del proyecto")