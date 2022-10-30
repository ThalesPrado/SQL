CREATE DATABASE db_sales;
USE db_sales;

-- Alterar nome da tabela
ALTER TABLE db_sales_market
RENAME tb_sales_market;

-- 1) Descubra porcentagem de venda por linha de produto em cada Branch
SELECT C.Branch,
	   C.Product_Line,
       C.city,
       C.Total,
       ROUND(SUM(C.Total) OVER (partition by C.Branch),2) AS total_Product,
       ROUND(C.Total * 100/SUM(C.Total) OVER (partition by C.Branch),2)  AS porcentagem_total
FROM tb_sales_market C
WHERE C.Branch = 'B'
GROUP BY C.Product_Line
ORDER BY C.Product_Line desc;

-- 2) Criar uma nova coluna com o total acumulado por dia
SELECT  
    C.Date,
    C.Quantity,
    SUM(C.Quantity) OVER (ORDER BY C.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado
FROM tb_sales_market C
ORDER BY C.Date;
	
-- 3) Calcule a mediana da quantidade vendida
with cte as (
  select quantity, 
    row_number() over (order by quantity) as r,
    count(quantity) over () as c 
  from tb_sales_market
),
median as (
  select quantity 
  from cte
  where r in (floor((c+1)/2), ceil((c+1)/2))
)
select avg(quantity) from median;

with cte as (
  select quantity, 
    row_number() over (order by quantity) as r,
    count(quantity) over () as c 
  from tb_sales_market
),
median as (
  select quantity 
  from cte
  where r in (floor((c+1)/2), ceil((c+1)/2))
) SELECT * FROM median;

-- 4) Diferencas do rankeamento com rank, dense, row
SELECT *,
RANK () OVER (ORDER BY Rating DESC) AS Rank1,
DENSE_RANK () OVER (ORDER BY Rating DESC) AS Rank_Dense,
ROW_NUMBER () OVER (ORDER BY Rating DESC) AS Rank_Row_Number
FROM tb_sales_market;

-- 5) Retorne somente as linhas duplicadas por Rating
SELECT Rating,
	   COUNT(Rating) AS Total_Contagem
FROM tb_sales_market
GROUP BY Rating
HAVING COUNT(Rating) >1;

-- 6) Qual o maior score na base de dados no Branch A
WITH CTE AS
			(
			SELECT
				MAX(Rating)
			FROM tb_sales_market
			WHERE Branch = 'A'
			)
SELECT * FROM tb_sales_market 
WHERE Rating = (SELECT * FROM CTE)
AND Branch = 'A';

-- 7) Como Deletar linhas duplicadas
WITH CTE AS
(
SELECT Rating,
	   COUNT(Rating) AS Total_Contagem
FROM tb_sales_market
GROUP BY Rating
HAVING COUNT(Rating) >1
)
DELETE Rating FROM tb_sales_market 
WHERE Rating IN (SELECT Rating FROM CTE);

-- 8) Encontre Invoices com valores acima da media
WITH media_preco_unitario(avg_preco) AS
(
SELECT 
		AVG(Unit_price) As Media_Precos
FROM tb_sales_market
) 
SELECT Invoice_ID,
       Branch,
       Unit_price,
       ROUND(avg_preco,2) AS Media_Preco
FROM tb_sales_market,media_preco_unitario
WHERE Unit_price > avg_preco
ORDER BY Unit_price DESC;

-- 9) Quando o Valor tiver acima da Media Retornar bom quando tiver abaixo ruim
WITH media_preco_unitario(avg_preco) AS
(
SELECT 
		AVG(Unit_price) As Media_Precos
FROM tb_sales_market
) 
SELECT Invoice_ID,
       Unit_price,
       ROUND(avg_preco,2) AS Media_Preco,
       CASE WHEN Unit_price > ROUND(avg_preco,2) THEN "Bom"
			WHEN Unit_price < ROUND(avg_preco,2) THEN "Mau"
            END AS Flag
FROM tb_sales_market,media_preco_unitario
ORDER BY Unit_price DESC;

-- 10) Encontra todas as linhas onde o nome da cidade comeca com Y
SELECT * FROM tb_sales_market
WHERE City REGEXP '^Y';

-- 11) Encontra todas as linhas onde contem a letra Y no nome da cidade
SELECT * FROM tb_sales_market
WHERE City REGEXP 'Y';

-- 12) Retornar apenas as linhas nas quais as vendas do periodo seguinte sao maiores que o periodo anterior
WITH tb_flag AS
(SELECT Date,
       Product_line,
       Total,
	   LEAD (Total,1,Total+1) OVER (PARTITION BY Product_Line ORDER BY Date) AS Total_Proximo,
       CASE WHEN Total < LEAD (Total) OVER (PARTITION BY Product_Line ORDER BY Date)
       THEN 1 ELSE 0
       END AS Flag
FROM tb_sales_market
)
SELECT Date,
	   Product_line,
	   Total,
       Flag
FROM Tb_flag
WHERE Flag = 1;

-- 13) Retornar apenas as linhas nas quais as vendas do periodo anterior sao maiores que o periodo atual
WITH Tb_Flag AS (SELECT Date,
       Product_line,
       Total,
	   LAG (Total) OVER (PARTITION BY Product_Line ORDER BY Date) AS Total_Anterior,
       CASE WHEN Total < LAG (Total) OVER (PARTITION BY Product_Line ORDER BY Date)
       THEN 1 ELSE 0
       END AS Flag
FROM tb_sales_market)
SELECT Date,
	   Product_line,
	   Total,
       Flag
FROM Tb_Flag
WHERE Flag = 1;

-- 15) Comparar vendas de um ano vs ano anterior mesmo mes
SELECT YEAR(Date) AS Ano,
		month(Date) AS Mes,
		SUM(Total) AS Total_Vendas,
        LAG(SUM(Total),12) OVER (PARTITION BY YEAR(Date) ORDER BY DATE) AS Mesmo_Mes_Ano_Anterior
FROM tb_sales_market
WHERE YEAR(Date) IN (2018,2019)
GROUP BY YEAR(Date),month(Date);

-- 16) Encontra vendas com valores mais proximos da media em um branch
SELECT  tb_sales_market.Branch,
		Invoice_ID,
        CEIL(Total) AS Round_UP,
        Avg_Total,
        ROUND(Total - Avg_Total,2) AS Diferenca,
RANK() OVER (PARTITION BY tb_sales_market.Branch ORDER BY ABS(round(Total - Avg_Total,2))) AS total_dif
FROM tb_sales_market INNER JOIN
(SELECT Branch, ROUND(AVG(Total),2) AS Avg_Total FROM tb_sales_market
GROUP BY Branch) As Avg_Total
ON tb_sales_market.Branch = Avg_Total.Branch;

-- 17) Verificar vendas por ID em cada data

SET @startdate = '2022-01-01';
SET @enddate = '2022-01-31';

WITH DATES AS
(
   SELECT @startdate AS OrderDate
   UNION ALL
   SELECT date_add(Date,1,OrderDate)
FROM DATES
WHERE date_add(Date,1,OrderDate) <= @enddate
)
SELECT DATES.OrderDate 
FROM DATES
LEFT JOIN tb_sales_market T2
ON DATES.OrderDate = T2.Dates ;

-- 18) Descubra a movimentacao do estoque em 0 - 90,180 and etc.
create table warehouse
(
ID varchar(10),
OnHandQuantity int,
OnHandQuantityDelta int,
event_type varchar(10),
event_datetime timestamp
);

insert into warehouse values
('SH0013', 278, 99 , 'OutBound', '2020-05-25 0:25'),
('SH0012', 377, 31 , 'InBound', '2020-05-24 22:00'),
('SH0011', 346, 1 , 'OutBound', '2020-05-24 15:01'),
('SH0010', 346, 1 , 'OutBound', '2020-05-23 5:00'),
('SH009', 348, 102, 'InBound', '2020-04-25 18:00'),
('SH008', 246, 43 , 'InBound', '2020-04-25 2:00'),
('SH007', 203, 2 , 'OutBound', '2020-02-25 9:00'),
('SH006', 205, 129, 'OutBound', '2020-02-18 7:00'),
('SH005', 334, 1 , 'OutBound', '2020-02-18 8:00'),
('SH004', 335, 27 , 'OutBound', '2020-01-29 5:00'),
('SH003', 362, 120, 'InBound', '2019-12-31 2:00'),
('SH002', 242, 8 , 'OutBound', '2019-05-22 0:50'),
('SH001', 250, 250, 'InBound', '2019-05-20 0:45');

WITH CTE AS (
			SELECT * FROM warehouse
			ORDER BY event_datetime DESC
			),
	DAYS AS(
			SELECT OnHandQuantity, event_datetime,
            date_sub(event_datetime,interval 90 DAY) AS Day90,
            date_sub(event_datetime,interval 180 DAY) AS Day180,
            date_sub(event_datetime,interval 270 DAY) AS Day270,
            date_sub(event_datetime,interval 365 DAY) AS Day365
            FROM CTE LIMIT 1
			),
            invetario90 AS
            (
            SELECT 
                  SUM(OnHandQuantityDelta) AS Total90days
                  FROM CTE CROSS JOIN
                  DAYS D
			WHERE event_type = 'InBound'
            AND CTE.event_datetime >= D.Day90
            ),
            invetario90Final AS
            (
            SELECT 
                CASE WHEN T.Total90days > D.OnHandQuantity
                THEN  D.OnHandQuantity ELSE T.Total90days
                END AS Total90days
            FROM invetario90 T
            CROSS JOIN DAYS D
            ),
            invetario180 AS
            (
            SELECT 
                  SUM(OnHandQuantityDelta) AS Total180days
                  FROM CTE CROSS JOIN
                  DAYS D
			WHERE event_type = 'InBound'
            AND CTE.event_datetime BETWEEN  D.Day180 AND D.Day90
            ),
			invetario270 AS
            (
            SELECT 
                  coalesce(SUM(OnHandQuantityDelta),0) AS Total270days
                  FROM CTE CROSS JOIN
                  DAYS D
			WHERE event_type = 'InBound'
            AND CTE.event_datetime BETWEEN  D.Day270 AND D.Day180
            )
SELECT 
      T1.Total90days,
      T2.Total180days,
      T3.Total270days
FROM invetario90Final T1
CROSS JOIN invetario180 T2
CROSS JOIN invetario270 T3;

-- 17V2) Verificar totais acumulados de estoque vs ano anterior

SELECT  YEAR(event_datetime) AS Ano,
		month(event_datetime) AS Mes,
		SUM(OnHandQuantity) AS Total_Vendas,
        LAG(SUM(OnHandQuantity),12) OVER (PARTITION BY YEAR(event_datetime) ORDER BY month(event_datetime)) AS Mesmo_Mes_Ano_Anterior
FROM warehouse
WHERE YEAR(event_datetime) IN (2019,2020)
GROUP BY YEAR(event_datetime),month(event_datetime);

-- 19) Insere Valores na Tabela warehousefinal de acordo com os dados da tabela warehouse
create table warehouse2
(
ID varchar(10),
OnHandQuantity int,
OnHandQuantityDelta int,
event_type varchar(10),
event_datetime timestamp
);

INSERT INTO warehouse2
SELECT * FROM warehouse;

SELECT * FROM warehouse2;

-- 20) Mude a quantidade de 99 para 100 no ID SH0013
UPDATE warehouse2
SET OnHandQuantityDelta = 100
WHERE ID = 'SH0013';
SET SQL_SAFE_UPDATES = 0; -- Ajustando para realizar o update
SHOW VARIABLES LIKE "sql_safe_updates"; -- Mostrando se ja esta efetivo

-- 21) Calcular totais por mes, ano e quarter em apenas uma query

SELECT 
        YEAR(event_datetime) AS Ano,
        month(event_datetime) AS mes,
        CASE WHEN MONTH(event_datetime) IN(1,2,3) THEN 1
			 WHEN MONTH(event_datetime) IN(4,5,6) THEN 2
             WHEN MONTH(event_datetime) IN(7,8,9) THEN 3
        ELSE 4
        END AS Trimestre,
        SUM(OnHandQuantity) AS Total
FROM warehouse
GROUP BY YEAR(event_datetime)
UNION ALL
SELECT 
       YEAR(event_datetime) AS Ano,
        month(event_datetime) AS mes,
		CASE WHEN MONTH(event_datetime) IN(1,2,3) THEN 1
			 WHEN MONTH(event_datetime) IN(4,5,6) THEN 2
             WHEN MONTH(event_datetime) IN(7,8,9) THEN 3
        ELSE 4
        END AS Trimestre,
        SUM(OnHandQuantity) AS Total
FROM warehouse
GROUP BY MONTH(event_datetime);


-- 22) Calcular media movel dos ultimos tres dias por cidade

SELECT 	Date,
		city,
		Total,
        ROUND(AVG(Total) OVER (Partition by city ORDER BY DATE ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING),1)
        AS mov_avg_3
From tb_sales_market
ORDER BY city;

-- 23) Trazer a segunda venda mais recente por ID
WITH CTE AS(
			SELECT *,
			ROW_NUMBER() OVER(partition by ID order by event_datetime) AS indexrow,
			COUNT(*) OVER(partition by ID order by event_datetime) AS contagem_errada,
			COUNT(*) OVER(partition by ID order by event_datetime 
						  RANGE BETWEEN UNBOUNDED PRECEDING 
						  AND UNBOUNDED FOLLOWING) AS contagem_certa
FROM warehouse2)
SELECT * FROM CTE
WHERE indexrow = CASE WHEN contagem_certa = 1 THEN 1 ELSE contagem_certa - 1 END 
;

-- 24) Faca um unpivot da tabela
CREATE VIEW unpivottable AS (
		SELECT  event_type,
		OnHandQuantity,
        CASE WHEN event_type = "OutBound" THEN OnHandQuantity END AS OutBound,
		CASE WHEN event_type = "InBound" THEN OnHandQuantity END AS InBound
	    FROM warehouse2
        );
SELECT * FROM unpivottable;

CREATE VIEW unpivottable2 AS(
			SELECT event_type,
					SUM(OutBound) AS Outbound,
                    SUM(InBound) AS Inbound
                    FROM unpivottable
			GROUP BY event_type
);
SELECT * FROM unpivottable2;

CREATE VIEW unpivottable3 AS (
			SELECT event_type,
                   coalesce(SUM(OutBound),0),
                   coalesce(SUM(InBound),0) 
			FROM unpivottable2
);
SELECT * FROM unpivottable3;

-- 25) Formatar mascara do CPF e do CNPJ
create table tb_pessoas
(
Nome varchar(20),
CPF varchar(50),
CNPJ varchar(50)
);
insert into tb_pessoas values
('Thales Prado','123.456.789-00','00.123.456/0001-00');

SELECT * FROM tb_pessoas;

SET SQL_SAFE_UPDATES = 0;
UPDATE tb_pessoas
SET CPF = REPLACE(CPF,'.','');
UPDATE tb_pessoas
SET CPF = REPLACE(CPF,'-','');

UPDATE tb_pessoas
SET CPF = INSERT(CPF,10,0,'-');
UPDATE tb_pessoas
SET CPF = INSERT(CPF,7,0,'.');
UPDATE tb_pessoas
SET CPF = INSERT(CPF,4,0,'.');

UPDATE tb_pessoas
SET CNPJ = REPLACE(CNPJ,'.','');
UPDATE tb_pessoas
SET CNPJ = REPLACE(CNPJ,'-','');

UPDATE tb_pessoas
SET CNPJ = INSERT(CNPJ,10,0,"-");
UPDATE tb_pessoas
SET CNPJ = INSERT(CNPJ,5,0,"/");	