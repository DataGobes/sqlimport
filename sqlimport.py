import pandas as pd
import pyodbc

server = 's060a0408\\dwh'
username = 'Python_User'
password = 'Unity_2018'
database = 'Datamart'
driver= '{ODBC Driver 13 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)

qry = '''WITH q AS (
	SELECT DATEPART(YEAR, ov.VervaldatumOverzichtsFactuur) AS Jaar
		,DATEPART(WEEK, ov.VervaldatumOverzichtsFactuur) AS [Week]
		,ov.OverzichtsfactuurId AS OverzichtsfactuurNr
		,Boekingnummer

		,ov.Factuurtype
		,ov.VervaldatumOverzichtsFactuur
		,ov.StartdatumOverzichtsFactuur
		,ov.EinddatumOverzichtsFactuur
		,Betalingsmethode
		,1 AS AantalFacturen
        ,CASE
			WHEN ov.VervaldatumOverzichtsfactuur <= Getdate()
				THEN 1
			ELSE 0
			END AS Vervallen
		,CASE
			WHEN vo.DatumAfgesloten = '1900-01-01'
				THEN NULL
			ELSE vo.DatumAfgesloten
			END AS DatumAfgesloten
		,CASE
			WHEN vo.DatumAfgesloten <> '1900-01-01'
				THEN DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten)
			END AS dagentelaat
		,CASE
			WHEN vo.DatumAfgesloten <> '1900-01-01'
				AND vo.DatumAfgesloten <= DATEADD(DAY, 5, VervaldatumOverzichtsFactuur)
				THEN 1
			ELSE 0
			END AS AantalOpTijd
		,KlantnummerDebiteur
		,Debiteur
		,cast(vo.BoekingBedrag as money) as boekingbedrag
		,CAST(vo.vereffendbedrag as money) as vereffendbedrag
		,CASE

	 when vo.DatumAfgesloten = '1900-01-01' then 'z. Onbetaald'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 0 then 'a. <0'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 6 then 'b. 0-5'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 8 then 'c. 6-7'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 15 then 'd. 8-14'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 22 then 'e. 15-21'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 29 then 'f. 22-28'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 61 then 'g. 29-60'
	 when DATEDIFF(DAY, ov.VervaldatumOverzichtsFactuur, vo.DatumAfgesloten) < 91 then 'h. 61-90'
	 else 'i. 90+'
	 end as bucket_vervallendagen

	 , case

	 when vo.boekingbedrag < 0 then 'a. <0'
	 when vo.boekingbedrag < 100 then 'b. 0-100'
	 when vo.boekingbedrag < 200 then 'c. 100-200'
	 when vo.boekingbedrag < 300 then 'd. 200-300'
	 when vo.boekingbedrag < 400 then 'e. 300-400'
	 when vo.boekingbedrag < 500 then 'f. 400-500'
	 when vo.boekingbedrag < 600 then 'g. 500-600'
	 when vo.boekingbedrag < 700 then 'h. 600-700'
	 when vo.boekingbedrag < 800 then 'i. 700-800'
	 when vo.boekingbedrag < 900 then 'j. 800-900'
	 when vo.boekingbedrag < 1000 then 'k. 900-1000'
	 else 'l. >1000' end as bucket_bedrag

	FROM dm_fact_fin_Vorderingen vo
	JOIN dm_dim_fin_OverzichtsFactuur ov ON (vo.OverzichtsFactuur_Id = ov.OverzichtsFactuur_Id)
	JOIN dm_dim_fin_Factuur fa ON (vo.Factuur_Id = fa.Factuur_Id)
	JOIN dm_dim_klnt_Debiteur deb ON (vo.Debiteur_Id = deb.Debiteur_Id)
	--JOIN Datawarehouse.dbo.dwh_Vereffening v on b.dwhrec_LogischeSleutel = v.LS_Boeking



	WHERE VervaldatumOverzichtsFactuur >= '2017-01-01'

		--and fa.dwhrec_IsRecent = 1
		--and deb.dwhrec_IsRecent = 1
	),

Afl AS
(
select vo.Boekingnummer, vo.Boekstuknummer, vo.Bankrekening, cast(vo.BoekingBedrag as money) as BoekingBedrag,

CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%afb%' then SETTLEAMOUNTCUR else 0 end) as money) as Afgeboekt,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%B2B%' then SETTLEAMOUNTCUR else 0 end) as money) as Verrekend,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%Ama%' then SETTLEAMOUNTCUR else 0 end) as money) as MatchAutomatisch,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%Mma%' then SETTLEAMOUNTCUR else 0 end) as money) as MatchHandmatig,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%TDE%' then SETTLEAMOUNTCUR else 0 end) as money) as Terugbetaling,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%Div%' then SETTLEAMOUNTCUR else 0 end) as money) as Correctie,
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '0000%' then SETTLEAMOUNTCUR else 0 end) as money) as '0000',
CAST(SUM(case when s.OFFSETTRANSVOUCHER like '%DBE%' then SETTLEAMOUNTCUR else 0 end) as money) as Incasso

from dm_fact_fin_Vorderingen vo
LEFT JOIN staging.dbo.stg_mec_Settlement s on vo.Boekingnummer = s.TRANSRECID
--LEFT JOIN Datawarehouse.dbo.dwh_Boeking b on vo.Boekingnummer = b.dwhrec_LogischeSleutel
----where Boekingnummer = 5640014299
--AND b.dwhrec_IsRecent=1
--AND b.dwhrec_IsDeleted=0

group by vo.Boekingnummer, vo.Boekstuknummer, vo.Bankrekening, vo.BoekingBedrag
)

select * from q
LEFT JOIN Afl on q.Boekingnummer = afl.Boekingnummer'''

a = pd.read_sql(qry, cnxn)

print(a)






