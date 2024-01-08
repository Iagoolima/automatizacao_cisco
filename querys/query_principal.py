import psycopg2

#query mocada, precisa incluir a query original e tambem a taxa 

def query_principal(data_inicio, data_final):
    
    
    with psycopg2.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='4321',
        database='cisco'
    ) as conexao:
        with conexao.cursor() as cursor:
            #query_sql = '''SELECT * FROM processos WHERE data BETWEEN %s and %s'''
            query_sql = '''SELECT
	'' as ACTIVITY_SEQUENCE_ID_I, --A
	'' as D2L_SHIPMENT_ID_I, --B
	CASE
		PROCESSO.tpproduto -- indica o tipo do processo
		WHEN '02' 
			THEN TRUNC(
					(
						( SELECT P_CHARGE_L.Vlrpesobruto FROM PROCESSO as P_CHARGE_L
							WHERE P_CHARGE_L.cdprocessointegracao LIKE PROCESSO.cdprocessointegracao and P_CHARGE_L.tpproduto like '04' limit 1
						) * 2.20462
					), 2
				)	
		ELSE
			TRUNC((PROCESSO.Vlrpesobruto * 2.20462), 2)	
	END ORG_WT_FROM_CARRIER_LB_I, --C
	
	CASE
		PROCESSO.tpproduto
		WHEN '02' 
			THEN TRUNC(
					(
						SELECT P_CHARGE_L.Vlrpesobruto FROM PROCESSO as P_CHARGE_L
							WHERE P_CHARGE_L.cdprocessointegracao LIKE PROCESSO.cdprocessointegracao and P_CHARGE_L.tpproduto like '04' limit 1
						
					), 2
				)	
		ELSE
			TRUNC(PROCESSO.Vlrpesobruto, 2)	
	END ORG_WT_FROM_CARRIER_KG_I, --D
	
	CASE
		PROCESSO.tpproduto
		WHEN '02' 
			THEN TRUNC(
					(
						( SELECT P_CHARGE_L.Vlrpesotaxado FROM PROCESSO as P_CHARGE_L
							WHERE P_CHARGE_L.cdprocessointegracao LIKE PROCESSO.cdprocessointegracao and P_CHARGE_L.tpproduto like '04' limit 1
						) * 2.20462
					), 2
				)	
		ELSE
			TRUNC(
				(
					( SELECT P_CHARGE_L.Vlrpesotaxado FROM PROCESSO as P_CHARGE_L
						WHERE translate(P_CHARGE_L.NRCONHECMASTER, '- ', '') LIKE translate(PROCESSO.NRCONHECMASTER, '- ', '') 
						AND P_CHARGE_L.NRCONHECIMENTO LIKE PROCESSO.NRCONHECIMENTO AND P_CHARGE_L.TPPRODUTO NOT LIKE '01' AND P_CHARGE_L.TPPRODUTO NOT LIKE '02' limit 1
					) * 2.20462
				), 2
			)	
	END CHARGEABLE_WEIGHT_LB_I, --E
 
	CASE
		PROCESSO.tpproduto
		WHEN '02' 
			THEN TRUNC(
					(
						SELECT P_CHARGE.Vlrpesotaxado FROM PROCESSO as P_CHARGE 
							WHERE P_CHARGE.cdprocessointegracao LIKE PROCESSO.cdprocessointegracao and P_CHARGE.tpproduto like '04' limit 1
					), 2	
				)	
		ELSE
			TRUNC(
				(
					SELECT P_CHARGE.Vlrpesotaxado FROM PROCESSO as P_CHARGE 
						WHERE translate(P_CHARGE.NRCONHECMASTER, '- ', '') LIKE translate(PROCESSO.NRCONHECMASTER, '- ', '') 
								AND P_CHARGE.NRCONHECIMENTO = PROCESSO.NRCONHECIMENTO 
								AND P_CHARGE.TPPRODUTO NOT LIKE '01' AND P_CHARGE.TPPRODUTO NOT LIKE '02' limit 1
				), 2	
			)	
	END CHARGEABLE_WEIGHT_KG_I, --F
	
	CASE
		PROCESSO.tpproduto
		WHEN '02' 
			THEN (select agenc.Vlrqtdevolume from processo agenc 
				  where agenc.cdprocessointegracao like PROCESSO.cdprocessointegracao and tpproduto like '04' limit 1)
		ELSE 
			TRUNC(PROCESSO.Vlrqtdevolume)
	END as PIECES_I, --G
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '02' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'C' -- Exp collect
			THEN 'GIANT1'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '02' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'P' -- Exp prepaid
			THEN 'GIANT2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '01' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'C' -- Imp collect
			THEN 'GIANT2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '01' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'P' -- Imp prepaid
			THEN 'GIANT1'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0
			THEN 'GIANT1'
		ELSE
			'GIANT2'
	END as CARRIER_ID_I, --H
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '02' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'C' -- Exp collect
			THEN 'GIANT CARGO MTZ'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '02' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'P' -- Exp prepaid
			THEN 'GIANT TRANSPORTES NACIONAIS E INTERNACIONAIS EIRELI'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '01' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'C' -- Imp collect
			THEN 'GIANT TRANSPORTES NACIONAIS E INTERNACIONAIS EIRELI'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND PROCESSO.tpproduto = '01' 
			AND CONTACORRENTEEMBARQUE.tipo_frete = 'P' -- Imp prepaid
			THEN 'GIANT CARGO MTZ'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0
			THEN 'GIANT CARGO MTZ'
		ELSE
			'GIANT TRANSPORTES NACIONAIS E INTERNACIONAIS EIRELI'
	END as CARRIER_NAME_I, --I
	
	CASE
		PROCESSO.tpproduto
		WHEN '01' 
			THEN (select to_char(dtregistrodi,'MM/DD/YYYY') from dicapa where idprocesso = PROCESSO.idprocesso limit 1)
		WHEN '02' 
			THEN (select to_char(dtregistrodue,'MM/DD/YYYY') from processoexportacao where idprocesso = PROCESSO.idprocesso limit 1)
		WHEN '03' 
			THEN (select to_char(dtembarque,'MM/DD/YYYY') from processo where idprocesso = PROCESSO.idprocesso limit 1)
		WHEN '04' 
			THEN (select to_char(dtembarque,'MM/DD/YYYY') from processo where idprocesso = PROCESSO.idprocesso limit 1)
	END ACTUAL_SHIP_DATE_I, --J
	
	CASE
		PROCESSO.tpproduto
		WHEN '01'
			THEN (select to_char(dtrealizacao,'MM/DD/YYYY') from followupprocesso where idprocesso = PROCESSO.idprocesso and idevento = 106 limit 1) --COLETA NO MULTILOG E ENTREGA NO DEPOSITO BR! (UPS)
	END as DELIVERY_DATE_I, --K
	
	to_char(FATURAMENTO.dtemissaonotaservico,'MM/DD/YYYY') as INVOICE_DATE_I, --L
	to_char(FATURAMENTO.dtemissaonotaservico,'MM/DD/YYYY') as INVOICE_RECEIPT_DATE_I, --M
	'193052350' as BILL_TO_ACCOUNT_I, --N
	'LATAM' as BILL_TO_THEATER_I, --O
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (7, 61, 47, 40, 119, 143, 150, 393, 83)
			THEN '193-000-052350-68996-000-000000'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (339, 88, 355, 524, 138)
			THEN '193-000-052350-68999-000-000000'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN ( 6, 122, 245, 479, 239, 8, 185, 354, 238, 445, 243, 19, 20, 16, 18, 124, 0)
			THEN '193-000-052350-68995-000-000000'
	END as DEPARTMENT_CODE_I, --P
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0
			THEN PROCESSO.NRPROCESSO
		ELSE
			FATURAMENTO.nrrps
	END as INVOICE_NUMBER_I, --Q
	
	PROCESSO.NRCONHECIMENTO as SHIPMENT_NUMBER_I, --R
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'BR1'
		WHEN PROCESSO.dtabertura >= '27/04/2023'
			THEN (
				select rc.nrreferenciacliente as unidade 
				from referenciacliente AS rc 
				where rc.idprocesso = PROCESSO.idprocesso 
				and rc.idreferenciacliente = 2 -- segunda referÃªncia
			)
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'U06'
				WHEN 186 -- CVG
					THEN 'U98'
				WHEN 240 -- GDL
					THEN 'T29'
				WHEN 91 -- BKK
					THEN 'T16'
				END
			)
	END as ORIGIN_NAME_I, --S
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'CISCO SISTEMAS HARDWARE E SOFTWARE LTD'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'CISCO SYSTEMS INC. C/O RYDER'
				WHEN 186 -- CVG
					THEN 'CISCO SYSTEMS INC C/O FLEXTRONICS AMERICA, LLC.'
				WHEN 240 -- GDL
					THEN 'FLEXTRONICS MANUFACTURING MEX S.A. de C.V.'
				WHEN 91 -- BKK
					THEN 'FABRINET CO., LTD.'
				END
			)
	END as ORIGIN_COMPANY_I, --T

	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'AVENIDA DAS NAÃ‡Ã•ES UNIDAS 12901 26Âº ANDAR'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN '724 HENRIETTA CREEK ROAD'
				WHEN 186 -- CVG
					THEN '4900 CREEKSIDE PARKWAY'
				WHEN 240 -- GDL
					THEN 'CARRETERA BASE AEREA #5850-4, COL. LA MORA, ZAPOPAN'
				WHEN 91 -- BKK
					THEN '475/2 MOO 7, TAMBOL KLONGKIEW, AMPHUR BANBUENG'
				END
			)
	END as ORIGIN_ADDRESS_I, --U
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'SÃƒO PAULO'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'ROANOKE'
				WHEN 186 -- CVG
					THEN 'LOOCKBOURNE'
				WHEN 240 -- GDL
					THEN 'GUADALAJARA'
				WHEN 91 -- BKK
					THEN 'ZAPOPAN'
				END
			)
	END as ORIGIN_CITY_I, --V
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'SP'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'TX'
				WHEN 186 -- CVG
					THEN 'OH'
				WHEN 240 -- GDL
					THEN 'JA'
				WHEN 91 -- BKK
					THEN '20'
				END
			)
	END as ORIGIN_STATE_CODE_I, --W
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN '04578-000'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN '76262'
				WHEN 186 -- CVG
					THEN '43137'
				WHEN 240 -- GDL
					THEN '45136'
				WHEN 91 -- BKK
					THEN '20220'
				END
			)
	END as ORIGIN_ZIP_I, --X
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN  'BR'
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'US'
				WHEN 186 -- CVG
					THEN 'US'
				WHEN 240 -- GDL
					THEN 'MX'
				WHEN 91 -- BKK
					THEN 'TH'
				END
			)
	END as ORIGIN_COUNTRY_CODE_I, --Y
	
	(
		select cdterminalcarga
			from terminalcarga
			where idterminalcarga = PROCESSO.Idlocalembarque
	) as ORIGIN_AIRPORT_I, --Z
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'U06'
				WHEN 186 -- CVG
					THEN 'U98'
				WHEN 240 -- GDL
					THEN 'T29'
				WHEN 91 -- BKK
					THEN 'T16'
				END
			)
		ELSE 
			'BR1'
	END as DESTINATION_NAME_I, --AA
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'CISCO SYSTEMS INC. C/O RYDER'
				WHEN 64 -- AUS
					THEN 'CISCO SYSTEMS INC C/O FLEXTRONICS AMERICA, LLC.'
				WHEN 240 -- GDL
					THEN 'FLEXTRONICS MANUFACTURING MEX S.A. de C.V.'
				WHEN 91 -- BKK
					THEN 'FABRINET CO., LTD.'
				END
			)
		ELSE 
			'CISCO SISTEMAS HARDWARE E SOFTWARE LTD'
	END as DESTINATION_COMPANY_I, --AB
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN '724 HENRIETTA CREEK ROAD'
				WHEN 64 -- AUS
					THEN '9500 METRIC BLVD, SUITE 200'
				WHEN 240 -- GDL
					THEN 'CARRETERA BASE AEREA #5850-4, COL. LA MORA, ZAPOPAN'
				WHEN 91 -- BKK
					THEN '475/2 MOO 7, TAMBOL KLONGKIEW, AMPHUR BANBUENG'
				END
			)
		ELSE 
			'AVENIDA DAS NAÃ‡Ã•ES UNIDAS 12901 26Âº ANDAR'
	END as DESTINATION_ADDRESS_I, --AC
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'ROANOKE'
				WHEN 64 -- AUS
					THEN 'AUSTIN'
				WHEN 240 -- GDL
					THEN 'GUADALAJARA'
				WHEN 91 -- BKK
					THEN 'CHONBURI'
				END
			)
		ELSE 
			'SÃƒO PAULO'
	END as DESTINATION_CITY_I, --AD
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'TX'
				WHEN 64 -- AUS
					THEN 'TX'
				WHEN 240 -- GDL
					THEN 'JA'
				WHEN 91 -- BKK
					THEN '20'
				END
			)
		ELSE 
			'SP'
	END as DESTINATION_STATE_CODE_I, --AE
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN '76262'
				WHEN 64 -- AUS
					THEN '78758'
				WHEN 240 -- GDL
					THEN '45136'
				WHEN 91 -- BKK
					THEN '20220'
				END
			)
		ELSE 
			'04578-000'
	END as DESTINATION_ZIP_I, --AF
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'US'
				WHEN 64 -- AUS
					THEN 'US'
				WHEN 240 -- GDL
					THEN 'MX'
				WHEN 91 -- BKK
					THEN 'TH'
				END
			)
		ELSE 
			'BR'
	END  as DESTINATION_COUNTRY_CODE_I,	--AG

	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				select cdterminalcarga
					from terminalcarga
					where idterminalcarga = PROCESSO.Idlocaldesembarque 
			)
		ELSE 
			(
				select cdterminalcarga
					from terminalcarga
					where idterminalcarga =
					 (
						SELECT P_CHARGE.Idlocaldesembarque FROM PROCESSO as P_CHARGE 
							WHERE translate(P_CHARGE.NRCONHECMASTER, '- ', '') LIKE translate(PROCESSO.NRCONHECMASTER, '- ', '') 
								AND P_CHARGE.NRCONHECIMENTO = PROCESSO.NRCONHECIMENTO 
								AND P_CHARGE.TPPRODUTO NOT LIKE '01' AND P_CHARGE.TPPRODUTO NOT LIKE '02' 
						 		limit 1
					) 
			)
	END as DESTINATION_AIRPORT_I, --AH
	
	'' as PRODUCT_CHARGEABLE_WT_LB_I, --AI
	'' as PRODUCT_CHARGEABLE_WT_KG_I, --AJ
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 7
			THEN 'WHB'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 61
			THEN 'WHA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 47
			THEN 'WHC'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 40
			THEN 'AMS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 119
			THEN 'HHG'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 143
			THEN 'PUA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 150
			THEN 'SOA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 393
			THEN 'LAU'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 83
			THEN 'DPE'

		WHEN CONTACORRENTEEMBARQUE.IDITEM = 88
			THEN 'DES'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 339
			THEN 'DEL2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 355
			THEN 'DES2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 524
			THEN 'TRF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 138
			THEN 'OTH'

		WHEN CONTACORRENTEEMBARQUE.IDITEM = 6
			THEN 'ICMS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 122
			THEN 'ICMSR'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 245
			THEN 'SDES'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 479
			THEN 'CCF2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 239
			THEN 'NFS2'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 8 OR 
			(CONTACORRENTEEMBARQUE.tipo_frete is not null)
			THEN 'AGN'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (185, 354)
			THEN 'IOF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 238
			THEN 'SEL'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 445
			THEN 'SET'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 243
			THEN 'SCF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 19
			THEN 'COF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 20
			THEN 'CSLL'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 16
			THEN 'IRRF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 18
			THEN 'PIS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 124
			THEN 'ICMSC'
		WHEN CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0 -- se vlr estiver zerado nÃ£o serÃ¡ identificado! - nÃ£o enviar
			THEN 'SISCOMEX'
		WHEN CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN 'IPI'
		WHEN CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN 'PIS'
		WHEN CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN 'II'
		WHEN CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN 'ICMS'
		WHEN CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN 'COFINS'
	END as CARRIER_CHARGE_CODE_I, --AK
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 7
			THEN 'ARMAZENAGEM ZONA SECUNDARIA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 61
			THEN 'CAPATAZIA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 47
			THEN 'ARMAZENAGEM'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 40
			THEN 'AMS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 119
			THEN 'HANDLING'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 143
			THEN 'PUA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 150
			THEN 'SOA'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 393
			THEN 'LAUDO TECNICO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 83
			THEN 'ARMAZENAGEM - DAPE'

		WHEN CONTACORRENTEEMBARQUE.IDITEM = 88
			THEN 'DESCONSOLIDACAO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 339
			THEN 'DELIVERY FEE'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 355
			THEN 'SERVICO DE DESCONSOLIDACAO CISCO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 524
			THEN 'TRANSPORTE REMOCAO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 138
			THEN 'OTHERS CHARGES'

		WHEN CONTACORRENTEEMBARQUE.IDITEM = 6
			THEN 'ICMS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 122
			THEN 'ICMS A RECOLHER'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 245
			THEN 'SERVICO DE DESEMBARACO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 479
			THEN 'TAXA DESPACHO ADUANEIRO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 239
			THEN 'SERVICO EMISSAO NOTA FISCAL'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 8 OR 
			(CONTACORRENTEEMBARQUE.tipo_frete is not null)
			THEN 'AGENCIAMENTO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (185, 354)
			THEN 'IOF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 238
			THEN 'SERVICO EMISSAO DE LI'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 445
			THEN 'SERVICO EMISSAO DE LAUDO TECNICO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 243
			THEN 'SERVICO DE COLLECT FEE'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 19
			THEN 'COFINS - FATURAMENTO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 20
			THEN 'CSLL - FATURAMENTO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 16
			THEN 'IRRF'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 18
			THEN 'PIS - FATURAMENTO'
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 124
			THEN 'ICMS COMPLEMENTAR'
		--WHEN CONTACORRENTEEMBARQUE.IDITEM = 353
			--THEN 'CUSTOMS_ENTRY_FEE'
		WHEN CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0 -- se vlr estiver zerado nÃ£o serÃ¡ identificado! - nÃ£o enviar
			THEN 'CUSTOMS_ENTRY_FEE'
		WHEN CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN 'EXCISE_TAX'
		WHEN CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN 'SOCIAL_TAX'
		WHEN CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN 'DUTY_PAID'
		WHEN CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN 'INDIRECT_TAX_PAID'
		WHEN CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN 'MISC_TAX_PAID'
	END as CARRIER_CHARGE_CODE_DESC_I, --AL
	
	'' as D2L_CHARGE_CODE_I, --AM
	'' as D2L_CHARGE_DESC_I, --AN
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (7, 61, 47, 40, 119, 143, 150, 393, 83)
			THEN 'WHS'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (339, 88, 355, 524, 138)
			THEN 'FRT'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN ( 6, 122, 245, 479, 239, 8, 185, 354, 238, 445, 243, 19, 20, 16, 18, 124, 0)
			THEN 'DUT'
	END as CARRIER_SERVICE_TYPE_I, --AO
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (7, 61, 47, 40, 119, 143, 150, 393, 83)
			THEN 'WAREHOUSE'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (339, 88, 355, 524, 138)
			THEN 'TRANSPORTATION'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN ( 6, 122, 245, 479, 239, 8, 185, 354, 238, 445, 243, 19, 20, 16, 18, 124, 0)
			THEN 'DUTIES AND TAXES'
	END as CARRIER_SERVICE_TYPE_DESC_I, --AP
	
	'' as PA_VENDOR_SERVICE_TYPE_I, --AQ
	'' as PA_VENDOR_SRVCE_TYPE_DESC_I, --AR
	'' as AUDIT_RESULT_CLAIM_REASON_I, --AS
	'' as AUDIT_RESULT_I, --AT
	'' as CLAIM_AMOUNT_I, --AU
	'' as MATCH_RESULT_I, --AV
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (7, 61, 47, 40, 119, 143, 150, 393, 83)
			THEN 'WAREHOUSE'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (339, 88, 355, 524, 138)
			THEN 'AIR'
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN ( 6, 122, 245, 479, 239, 8, 185, 354, 238, 445, 243, 19, 20, 16, 18, 124, 0)
			THEN 'DUTY & TAX'
	END as MODE_OF_TRANSPORT_I, --AW
	
	(
		select po.nrreferenciacliente
		from referenciacliente as po
		where po.idprocesso = PROCESSO.idprocesso
		order by po.idreferenciacliente asc
		limit 1  -- primeira referÃªncia 
	) as ORDER_NUMBER_I, --AX
	
	(
		select po.nrreferenciacliente
		from referenciacliente as po
		where po.idprocesso = PROCESSO.idprocesso
		order by po.idreferenciacliente asc
		limit 1  -- primeira referÃªncia 
	) as PURCHASE_ORDER_I, --AY
	
	'' as TRANSACTION_TYPE_I, --AZ
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (16, 19, 20, 18)
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1), 2)
		--WHEN ITEMDESPESA.TPITEMDESPESA LIKE 'R'
			--THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1), 2)
		WHEN CONTACORRENTEEMBARQUE.vritem < 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1), 2)
		
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0 -- se vlr estiver zerado nÃ£o serÃ¡ identificado! - nÃ£o enviar
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotaltxsiscomex), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalipi), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalpis), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalii), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalicms), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotacofins), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_collect is not null 
			AND CONTACORRENTEEMBARQUE.frete_collect > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_collect), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_prepaid is not null 
			AND CONTACORRENTEEMBARQUE.frete_prepaid > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_prepaid), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_internacional is not null 
			AND CONTACORRENTEEMBARQUE.frete_internacional > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_internacional), 2)
		ELSE
			TRUNC(CONTACORRENTEEMBARQUE.vritem, 2)
	END as CARRIER_CHARGE_BILLED_AMT_I, --BA
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (16, 19, 20, 18)
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1), 2)
		WHEN CONTACORRENTEEMBARQUE.vritem < 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1), 2)
			
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0 -- se vlr estiver zerado nÃ£o serÃ¡ identificado! - nÃ£o enviar
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotaltxsiscomex), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalipi), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalpis), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalii), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalicms), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotacofins), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_collect is not null 
			AND CONTACORRENTEEMBARQUE.frete_collect > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_collect), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_prepaid is not null 
			AND CONTACORRENTEEMBARQUE.frete_prepaid > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_prepaid), 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_internacional is not null 
			AND CONTACORRENTEEMBARQUE.frete_internacional > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_internacional), 2)
			
		ELSE
			TRUNC(CONTACORRENTEEMBARQUE.vritem, 2)
	END as CARRIER_CHARGE_PAID_AMT_I, -- 
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (16, 19, 20, 18)
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.vritem < 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1) / valor da taxa, 2)
		
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotaltxsiscomex) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalipi) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalpis) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalii) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalicms) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotacofins) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_collect is not null 
			AND CONTACORRENTEEMBARQUE.frete_collect > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_collect) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_prepaid is not null 
			AND CONTACORRENTEEMBARQUE.frete_prepaid > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_prepaid) / valor da taxa, 2) 
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_internacional is not null 
			AND CONTACORRENTEEMBARQUE.frete_internacional > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_internacional) / valor da taxa, 2)
			
		ELSE
			TRUNC(CONTACORRENTEEMBARQUE.vritem / valor da taxa, 2)
	END as CARRIER_CHRG_BILL_AMT_USD_I,  --valor fixo 5,1679, serÃ¡ criado tabela na automatizaÃ§Ã£o --BC
	
	CASE
		WHEN CONTACORRENTEEMBARQUE.IDITEM IN (16, 19, 20, 18)
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.vritem < 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.vritem * -1) / valor da taxa, 2)
			
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotaltxsiscomex > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotaltxsiscomex) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalipi > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalipi) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalpis > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalpis) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalii > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalii) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotalicms > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotalicms) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.Vltotacofins > 0 
			THEN TRUNC((CONTACORRENTEEMBARQUE.Vltotacofins) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_collect is not null 
			AND CONTACORRENTEEMBARQUE.frete_collect > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_collect) / valor da taxa, 2)
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_prepaid is not null 
			AND CONTACORRENTEEMBARQUE.frete_prepaid > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_prepaid) / valor da taxa, 2) 
		WHEN CONTACORRENTEEMBARQUE.IDITEM = 0 AND CONTACORRENTEEMBARQUE.frete_internacional is not null 
			AND CONTACORRENTEEMBARQUE.frete_internacional > 0
			THEN TRUNC((CONTACORRENTEEMBARQUE.frete_internacional) / valor da taxa, 2)
			
		ELSE
			TRUNC(CONTACORRENTEEMBARQUE.vritem / valor da taxa, 2)
	END as CARRIER_CHRG_PAID_AMT_USD_I, --valor fixo 5,1679, serÃ¡ criado tabela na automatizaÃ§Ã£o --BD
	
	(
		select sigla
		from moeda
		where idmoeda = PROCESSO.idmoedamle
		limit 1
	) as BILLED_CURRENCY_I, --BE
	
	'BRL' as PAID_CURRENCY_I, --BF
	
	'' as RUN_DATE_I, --BG
	'' as CLIENT_RUN_ID_I, --BH
	'' as ACTIVITY_DATE_I, --BI
	'' as ACTIVITY_TYPE_I, --BJ
	
	(
		select po.nrreferenciacliente
		from referenciacliente as po
		where po.idprocesso = PROCESSO.idprocesso
		order by po.idreferenciacliente asc
		limit 1
	) as DELIVERY_ID_I, --BK
	
	'' as HTS_CODE_I, --BL
	'' as CMRCL_INVOICE_CURRENCY_I, --BM
	'' as CMRCL_INVOICE_PRICE_I, --BN
	'' as PRODUCT_ID_I, --BO
	'' as PRODUCT_BUSINESS_UNIT_I, --BP
	'' as PRODUCT_GROSS_WEIGHT_I, --BQ
	'' as PRODUCT_GW_UOM_I, --BR
	'' as AF_PIECES_I, --BS
	'' as PRODUCT_QTY_I, --BT
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN (
				CASE 
					PROCESSO.Idlocaldesembarque
				WHEN 1 --DFW
					THEN 'U06'
				WHEN 186 -- CVG
					THEN 'U98'
				WHEN 240 -- GDL
					THEN 'T29'
				WHEN 91 -- BKK
					THEN 'T16'
				END
			)
		ELSE 
			'BR1'
	END as DESTINATION_ORG_CODE_I, --BU
	
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04')
			THEN 'BR1'
		WHEN PROCESSO.dtabertura > '27/04/2023'
			THEN (
				select rc.nrreferenciacliente as unidade 
				from referenciacliente AS rc 
				where rc.idprocesso = PROCESSO.idprocesso 
				and rc.idreferenciacliente = 2 -- segunda referÃªncia
			)
		ELSE 
			(
				CASE 
					PROCESSO.Idlocalembarque
				WHEN 1 --DFW
					THEN 'U06'
				WHEN 186 -- CVG
					THEN 'U98'
				WHEN 240 -- GDL
					THEN 'T29'
				WHEN 91 -- BKK
					THEN 'T16'
				END
			)
	END as ORIGIN_ORG_CODE_I, --BV
	
	'' as CARRIER_CODE_I, --BW
	'' as OTM_PLANNED_CARRIER_I, --BX
	'' as ORDER_SERVICE_LEVEL_I, --BY
	'' as OTM_PLANNED_SERVIC_LEVEL_I, --BZ
	'' as OTM_PLANNED_MODE_I, --CA
	'' as ACTUAL_SHIPMENT_WEIGHT_I, --CB
	'' as OTM_PLANNED_SHIPMENT_WT_I, --CC
	'' as OTM_PLANNED_SHIPMENT_COST_I, --CD
	'' as OTM_RERATED_SHIPMENT_COST_I, --CE
	'' as TPL_HAND_OFF_DATE_I, --CF
	'' as IS_DGI_FGI_I,	--CG
	
	CASE
		(select rc.nrreferenciacliente as unidade from referenciacliente as rc where rc.idprocesso = PROCESSO.idprocesso and rc.idreferenciacliente = 3)
		WHEN 'NB%'
			THEN 'NB'
		WHEN 'RF%' 
			THEN 'RF'
		WHEN 'TEFR%' 
			THEN 'TEFR'
		ELSE
			(select rc.nrreferenciacliente as unidade from referenciacliente AS rc where rc.idprocesso = PROCESSO.idprocesso and rc.idreferenciacliente = 3)
	END ORDER_TYPE_I,  -- terceira referÃªncia --CH
	
	'' as EXPENSE_TYPE_I, --CI
	'' as GOV_FEE_CATEGORY_I, --CJ
	'' as LIST_PRICE_I, --CK
	'' as STANDARD_COST_I, --CL
	'' as OTM_PLANNED_WEIGHT_UOM_I, --CM
	'' as OTM_ACTUAL_WEIGHT_UOM_I, --CN
	
	(
		select po.nrreferenciacliente
		from referenciacliente as po
		where po.idprocesso = PROCESSO.idprocesso
		order by po.idreferenciacliente asc
		limit 1  -- primeira referÃªncia 
	) as INVOICE_REFERENCE_NUMBER_I, --CO
	
	PROCESSO.nrprocesso as processo,
	PROCESSO.idprocesso,
	PROCESSO.NRCONHECIMENTO,
	PROCESSO.NRCONHECMASTER,
	PROCESSO.CDPROCESSOINTEGRACAO,
	--PROCESSO.tpproduto,
	FATURAMENTO.IDFATURAMENTO,
	CONTACORRENTEEMBARQUE.IDITEM,
	CONTACORRENTEEMBARQUE.vritem,
	itemdespesa.nmitemdespesa AS item_despesa,
	--itemdespesa.tpitemdespesa,
	--FATURAMENTO.Vltotalirrf,
	--FATURAMENTO.VLRIN,
	
	CONTACORRENTEEMBARQUE.frete_collect,
	CONTACORRENTEEMBARQUE.frete_prepaid,
	CONTACORRENTEEMBARQUE.frete_internacional,
	CONTACORRENTEEMBARQUE.tipo_frete,
	
	-- a soma dos impostos ICMS, II, IPI, PIS, COFINS, TAXA SISCOMEX  convertidos de BRL para USD pela taxa de cÃ¢mbio da Cisco
	TRUNC(
		( 
			select ( DI.Vltotalicms + DI.Vltotalii + DI.Vltotalipi + DI.Vltotalpis + DI.Vltotacofins + DI.Vltotaltxsiscomex )
			from dicapa DI
			where DI.idprocesso = PROCESSO.idprocesso 
		) / valor da taxa
	, 2) as TOTAL_IMPOSTOS_DI_USD,
	
	
	--o valor do frete informado na DI convertido de BRL para USD pela taxa de cÃ¢mbio da Cisco
	CASE
		WHEN PROCESSO.tpproduto IN ('02', '04') 
			THEN TRUNC( PROCESSO.Vlrfreteinternacionalmneg / valor da taxa, 2)
		ELSE 
			TRUNC( PROCESSO.Vlrfretemnac / valor da taxa, 2)
	END as FRETE_DI_USD
	
FROM
	PROCESSO
JOIN
	FATURAMENTOPROCESSO ON FATURAMENTOPROCESSO.IDPROCESSO = PROCESSO.IDPROCESSO
JOIN
	FATURAMENTO ON FATURAMENTO.IDFATURAMENTO = FATURAMENTOPROCESSO.IDFATURAMENTO
--JOIN
	--CONTACORRENTEEMBARQUE ON CONTACORRENTEEMBARQUE.IDFATURAMENTO = FATURAMENTOPROCESSO.IDFATURAMENTO AND CONTACORRENTEEMBARQUE.IDPROCESSO = PROCESSO.IDPROCESSO
JOIN
	(
		SELECT --Nem todo processo terÃ¡ imposto, Ã© possÃ­vel aparecer linhas vazias se identificar o imposto com vlr zerado
			idprocesso, iddicapa, Vltotalii, 0 as Vltotalipi, 0 as Vltotalpis, 0 as Vltotalicms, 0 as Vltotacofins, 0 as Vltotaltxsiscomex, 0 as iditem,0 as IDFATURAMENTO,0 as vritem,0 as frete_collect, 0 as frete_prepaid, 0 as frete_internacional, '' as tipo_frete 
		from dicapa d1
		UNION
		SELECT 
			idprocesso, iddicapa, 0, Vltotalipi, 0, 0, 0, 0, 0,0,0,0,0,0,'' 
		from dicapa d2
		UNION
		SELECT 
			idprocesso,iddicapa,  0, 0, Vltotalpis, 0, 0, 0, 0,0,0,0,0,0,'' 
		from dicapa d3
		UNION
		SELECT 
			idprocesso,iddicapa,  0, 0, 0, Vltotalicms, 0, 0, 0,0,0,0,0,0,'' 
		from dicapa d4 
		UNION
		SELECT 
			idprocesso, iddicapa, 0, 0, 0, 0, Vltotacofins, 0, 0,0,0,0,0,0,'' 
		from dicapa d5 
		UNION
		SELECT 
			idprocesso, iddicapa, 0, 0, 0, 0, 0, Vltotaltxsiscomex, 0,0,0,0,0,0,'' 	
		from dicapa d6
		
		UNION
		SELECT
			idprocesso, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Vlrfretecollectmneg as frete_collect, Vlrfreteprepaidmneg as frete_prepaid, vlrfreteinternacionalmneg as frete_internacional, tpfrete as tipo_frete 
		from processo d7
		
		UNION
		SELECT 
			idprocesso, 0, 0, 0, 0, 0, 0, 0, iditem, IDFATURAMENTO, vritem,0,0,0,'' 
		from CONTACORRENTEEMBARQUE d9 
	) AS CONTACORRENTEEMBARQUE on CONTACORRENTEEMBARQUE.idprocesso = processo.idprocesso and CONTACORRENTEEMBARQUE.idfaturamento is not null

JOIN
	processo AS FRETE on FRETE.idprocesso = processo.idprocesso
	
	
LEFT JOIN
	ITEMDESPESA on ITEMDESPESA.iditemdespesa = CONTACORRENTEEMBARQUE.iditem
	
--JOIN
	--PROCESSO PROCESSO_AGEN ON ( translate(PROCESSO_AGEN.NRCONHECMASTER, '- ', '') = translate(PROCESSO.NRCONHECMASTER, '- ', '') 
						--AND PROCESSO_AGEN.NRCONHECIMENTO = PROCESSO_AGEN.NRCONHECIMENTO 
						--AND PROCESSO_AGEN.TPPRODUTO NOT LIKE '01' AND PROCESSO_AGEN.TPPRODUTO NOT LIKE '02'
		--)
WHERE
	PROCESSO.IDPESSOACLIENTE = 2 -- Fixo cliente CISCO SERVICOS
	and FATURAMENTO.nrrps is not null
	and PROCESSO.tpproduto in ('01', '02')
	--and processo.idprocesso = 808132  
	and FATURAMENTO.dtemissaonotaservico between ' valor data de incio' and 'valor data final'
	and 
		CONTACORRENTEEMBARQUE.IDITEM NOT IN (523, 9, 12) -- NÃ£o extrair itens de adiantamento, nem saldo a receber (exp)
ORDER BY PROCESSO.IDPROCESSO DESC'''
            try:
                #cursor.execute(query_sql, (data_inicio, data_final))
                cursor.execute(query_sql)
                print("Consulta executada com sucesso.")
                resultados = [row for row in cursor.fetchall()]  # Crie uma lista a partir dos resultados
            except Exception as e:
                print(f"Erro durante a execução da consulta: {e}")
                return None
    
    return resultados