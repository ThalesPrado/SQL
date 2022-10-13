
-- Database criado baseado na base de dados do Kaggle Supermarket Sales Data in Myanmar

CREATE DATABASE db_sales; -- Linha que cria meu database no mysql
USE db_sales; -- Linha que informa ao sql o database que quero utilizar

-- Alterar nome da tabela
ALTER TABLE db_sales_market -- Linha que alterei o nome da tabela para tb_sales_market
RENAME tb_sales_market; 

-- 1) Qual a porcentagem de venda por linha de produto no branch B ?
SELECT C.Branch, -- Seleciono as colunas Branch
	   C.Product_Line, -- Linha de produto
       C.city, -- Cidade
       C.Total, -- Total
       ROUND(SUM(C.Total) OVER (partition by C.Branch),2) AS total_Product, -- Nessa linha realizo o total de vendas no branch B porque quero utilizar esse total para descobrir o share de cada linha de produto
       ROUND(C.Total * 100/SUM(C.Total) OVER (partition by C.Branch),2)  AS porcentagem_total
       -- E aqui conseguimos a resposta para a pergunta onde temos o total por cada linha de produto
FROM tb_sales_market C -- Utilizei um alias para ficar mais facil na hora de chamar as colunas 
WHERE C.Branch = 'B' -- Aqui realizo o filtro do branch
GROUP BY C.Product_Line -- Aqui realizo o agrupamento dos totais pro linha de produto
ORDER BY C.Product_Line desc; -- E finalmente fazemos a ordenacao

-- 2) Qual acumulado por dia ?
SELECT  -- Seleciono as colunas de data e quantidade para fazer o check se de fato o calculo esta correto
    C.Date,
    C.Quantity,
    SUM(C.Quantity) OVER (ORDER BY C.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado -- Nessa janela que o calculo e realizado com os totais pensado que tambem poderiamos ter o calculo de MTD ou YTD seguindo alguma logica semelhante
FROM tb_sales_market C
ORDER BY C.Date;
	
-- 3) Qual a mediana da quantidade vendida ?
-- Criamos primeiro a CTE com quantidade e a funcao row_number cria o index para cada linha dentro dessa nossa janela
-- Depois invocamos a CTE dentro da noss consulta da mediana utilizando as instrucoes de floor que faz o arredondamento desse numero da coluna para baixo e ceil que faz o arredodamento para cima
-- Como essa consulta retorna apenas duas linhas tiramos a media aritmetica e achamos o valor da mediana
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

-- 4) Quais sao as diferencas de ranking com as funcoes rank, dense_rank e row_number ?

SELECT *,
RANK () OVER (ORDER BY Rating DESC) AS Rank1, -- Aqui como temos 5 linhas com o mesmo rating de 10 o proximo valor de rank e 6. Inves de seguir on ranking sequencial
DENSE_RANK () OVER (ORDER BY Rating DESC) AS Rank_Dense, -- Aqui temos todos as linhas com o rating de 10 como o primeiro do ranking porem inves de 6 nesse segundo caso o dense_rank segue a ordem sequencial para 2,3 assim sucessivamente
ROW_NUMBER () OVER (ORDER BY Rating DESC) AS Rank_Row_Number -- Aqui temos um numero diferente para cada linha independente dos valores. Mesmo as 5 primeiras linhas tendo um valor de 10 retornamos o valor de 1,2,3 seguindo uma ordem sequencial.
FROM tb_sales_market;

-- 5) Retornar apenas linhas duplicadas para cada rating
-- A Primeira parte do problema precisamos fazer uma contagem para cada rating dentro da nossa base de dados depois disso agrupamos com a clasula GROUP BY e uma vez que temos os totais filtamos com o having tudo que e acima de 1
SELECT Rating,
	   COUNT(Rating) AS Total_Contagem
FROM tb_sales_market
GROUP BY Rating
HAVING COUNT(Rating) >1;

-- 6) Retornar a linha com o maior rating no branch A
-- Primeiro usamos o MAX de rating na CTE no branch A
-- Depois retornamos essa linha com o total filtrando de acordo com o que temos na CTE
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

-- 7) Delete linhas com rating duplicados dentro do dataset
-- Primeiro na CTE temos o filto das linhas na quais temos duplicidades usando o having
-- Uma vez que temos esse subset chamamos a clausula where com valores dentro da lista com a clausula IN
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

-- 8) Retornar as invoices com valores acima da media
-- Na primeira CTE 'media_preco_unitario' queremos buscar a media de valores das invoice e dentro do paratenses temos o nome da coluna que posteriormente vamos referenciar dentro da nossa consulta
-- Agora que nossa media foi criado trazemos ela na nova consulta filtrando apenas valores onde o valor unitario e > que avg_precos e ordemos em ordem decrescente
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

-- 9) Quando o Valor tiver acima da media retornar algo e quando tiver abaixo da media retornar algo
-- Aqui temos o mesmo cenario da consulta acima com a diferenca que colocamos nosso IF para criar um flag do que esta acima da media e o que esta abaixo da media
WITH media_preco_unitario(avg_preco) AS
(
SELECT 
		AVG(Unit_price) As Media_Precos
FROM tb_sales_market
) 
SELECT Invoice_ID,
       Unit_price,
       ROUND(avg_preco,2) AS Media_Preco,
       CASE WHEN Unit_price > ROUND(avg_preco,2) THEN "Acima_Media"
			WHEN Unit_price < ROUND(avg_preco,2) THEN "Abaixo_Media"
            END AS Flag
FROM tb_sales_market,media_preco_unitario
ORDER BY Unit_price DESC;

-- 10) Encontra todas as linhas onde o nome da cidade comeca com Y
-- Aqui e necessario realizar um expressao regular para trazer todas as cidades que comecam com Y
SELECT * FROM tb_sales_market
WHERE City REGEXP '^Y';

-- 11) Encontra todas as linhas onde contem a letra Y no nome da cidade
SELECT * FROM tb_sales_market
WHERE City REGEXP 'Y';

-- 12) Retornar apenas as linhas nas quais as vendas do periodo seguinte sao maiores que o periodo anterior
-- Primeiro criamos uma CTE onde a clausula LEAD traz a total seguinte fazendo o partition pela linha de produto e ordenando pelas datas
-- Depois utilizamos o CASE WHEN para fazer noss flag se for > que a venda anterior 1 se nao for 0
-- Uma vez terminado essa logica trazemos as colunas que queremos filtrando todos os flag 1 
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
-- Mesma dinamica da query anterior com a diferenca que aqui usamos LAG para retornar o total anterior
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

-- 15) Faca uma analise de vendas do ano anterior no mesmo mes
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

-- 18) Descubra a movimentacao do estoque em 0 - 90,180 and etc

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
        ROUND(AVG(Total) OVER 
        (Partition by city ORDER BY DATE ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING),1)
        AS mov_avg_3
From tb_sales_market
ORDER BY city;