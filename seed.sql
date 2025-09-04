CREATE SCHEMA IF NOT EXISTS public;
DROP TABLE IF EXISTS public.customers_demo;
CREATE TABLE public.customers_demo (
  id SERIAL PRIMARY KEY,
  first_name TEXT, last_name TEXT, email TEXT,
  age INTEGER, income NUMERIC(12,2), signup_date DATE, is_active BOOLEAN
);
INSERT INTO public.customers_demo (first_name,last_name,email,age,income,signup_date,is_active) VALUES
('Ana','López','ana@example.com',28,18500.00,'2024-01-15',TRUE),
('Ana','López','ana@example.com',28,18500.00,'2024-01-15',TRUE),
('Luis','Pérez',NULL,35,NULL,'2023-11-02',FALSE),
('María','García','maria@example.com',NULL,32500.50,NULL,TRUE),
('Juan','Hernández','juan@example.com',41,42780.75,'2022-05-30',TRUE),
('Lucía','Santos',NULL,22,9800.00,'2025-02-11',FALSE),
('Diego','Ruiz','diego@example.com',29,15000.00,'2024-07-22',TRUE),
('Sofía','Martínez','sofia@example.com',33,22100.25,'2023-03-09',TRUE),
('Pedro','Núñez','pedro@example.com',51,50320.00,'2021-10-19',TRUE),
('Elena','Cruz','elena@example.com',27,NULL,'2024-12-01',FALSE);
CREATE INDEX IF NOT EXISTS idx_customers_demo_allcols ON public.customers_demo (first_name,last_name,email,age,income,signup_date,is_active);
