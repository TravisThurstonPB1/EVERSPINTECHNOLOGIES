--V6 report

Declare @date as date = '20211231' --/*Select T0.DocDate from IBT1 T0 Where T0.DocDate = */'[%0]'

Select *, @date 'Selection Date', Convert(date,Getdate(),112) 'AsOfDate'  -- This select statement takes the the following subqueries and displays the information in the final report.
from(  -- The sub query below returns the two union sub queries in a single sub query before the final report.
Select isnull(T0.ItemCode,'') 'ItemCode'
--, isnull(T1.WIPItem,'') 'WIPItem'
, isnull(T0.Family,'') 'Family'
, isnull(T0.SAPStage,'') 'Stage'
, isnull(T0.Whse, '') 'Whse'
, T0.AbsEntry
, isnull(T0.SAPLot,'') 'OnHand Lot'
, isnull(T0.SAPParentLot,'') 'OnHand ParentLot'
, isnull(T0.BatchAtt2,'') 'BatchAtt2'
, isnull(Cast(T0.AddmissionDate as Nvarchar),'') 'Lot AddmissionDate'
, datediff(dd,isnull(T0.AddmissionDate,''),@date) 'Age of Lot (Days)'
, isnull(T0.Onhand,0) 'OnHand/WIP Qty'
, isnull(T0.WO#,'') 'WO#'
, isnull(T0.SpinwebWONo,'') 'UTC_SpinwebNo'
, isnull(T0.PRDO_StartDate,'') 'PRDO_StartDate'
, isnull(T0.SAPCost, 0) 'PerUnitLotCost'
, T0.OnHand*T0.SAPCost 'TotalLotCost'
, isnull(T0.InvAccount,'') 'GLAccount'
From(-- Declare @Date date = '20210630'   -- This subquery returns the on hand inventory information
	Select Convert(date,T2.CreateDate,112) 'AddmissionDate'
	, T0.ItemCode 'ItemCode'
	, T2.Distnumber 'SAPLot'
	, Cast(T2.Notes as Nvarchar) 'SAPParentLot'
	, T2.LotNumber 'BatchAtt2'
	, T1.LocCode 'Whse'
	, Sum(T0.Quantity) 'OnHand'
	--  This Case statement is calculating the ON hand Cost and is looking for revaluation documents that could be affecting the cost as of the Date run --
	/*, case when isnull((Select  MAX(MRV3.DocEntry) from MRV3 With(nolock) inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry Where MRV3.SNBNum = T2.DistNumber and OMRV.CreateDate Between @Date and Dateadd(dd,25,@date) and OMRV.DocDate <= @date),'')<>'' 
			then (Case When (Select Max(Convert(date,IGE1.DocDate,112)) from IGE1 With(nolock) inner join OITL With(nolock) on IGE1.DocEntry = OITL.DocEntry inner Join ITL1 With(nolock) on OITL.LogEntry = ITL1.LogEntry and OITL.StockEff = 1 INNER JOIN OBTN With(nolock) on ITL1.ItemCode = OBTN.ItemCode and ITL1.SysNumber = OBTN.SysNumber Where OBTN.DistNumber = T2.DistNumber and OBTN.ItemCode = T0.ItemCode and IGE1.DocDate Between @date and Dateadd(dd, 25, @date)) < (Select MAX(Convert(date,OMRV.CreateDate,112)) from OMRV With(nolock) inner Join MRV3 With(nolock) on OMRV.DocEntry = MRV3.DocEntry Inner Join MRV1 With(nolock) on MRV1.DocEntry = OMRV.DocEntry Where MRV3.SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode) Then ISNULL((Select TOP 1 CurrCost from MRV3 With(nolock) inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry inner join MRV1 With(nolock) on OMRV.DocEntry = MRV1.DocEntry where SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and MRV3.DocEntry = (Select MAx(MRV3.DocEntry) 'DocEntry' from MRV3 With(nolock) inner join MRV1 With(nolock) on MRV3.DocEntry = MRV1.DocEntry inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry where MRV3.SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and OMRV.DocDate <= @date)), T2.CostTotal/ABS(SUM(T0.Quantity))) Else ISNULL((Select TOP 1 NewCost from MRV3 With(nolock) inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry inner join MRV1 With(nolock) on OMRV.DocEntry = MRV1.DocEntry where SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and MRV3.DocEntry = (Select MAx(MRV3.DocEntry) 'DocEntry' from MRV3 With(nolock) inner join MRV1 With(nolock) on MRV3.DocEntry = MRV1.DocEntry inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry where MRV3.SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and OMRV.DocDate <= @date)), T2.CostTotal/ABS(sum(T0.Quantity))) End)
		--When ISNULL((Select MAX(MRV3.DocEntry) from MRV3 with(nolock) inner join OMRV with(nolock) on MRV3.DocEntry = OMRV.DocEntry Where MRV3.SNBNum = T2.DistNumber and OMRV.CreateDate <= @date),'') <>'' then  ISNULL((Select TOP 1 NewCost from MRV3 With(nolock) inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry inner join MRV1 With(nolock) on OMRV.DocEntry = MRV1.DocEntry where SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and MRV3.DocEntry = (Select MAx(MRV3.DocEntry) 'DocEntry' from MRV3 With(nolock) inner join MRV1 With(nolock) on MRV3.DocEntry = MRV1.DocEntry inner join OMRV With(nolock) on MRV3.DocEntry = OMRV.DocEntry where MRV3.SNBNum = T2.DistNumber and MRV1.ItemCode = T0.ItemCode and OMRV.DocDate <= @date)), T2.CostTotal/sum(T0.Quantity))
		when T2.Quantity = 0 then Cast(isnull((Case when T2.CostTotal = 0 then NULL else T2.CostTotal end)/Sum(T0.Quantity),0)as Money) else Cast(isnull((Case when T2.CostTotal = 0 then NULL else T2.CostTotal end)/T2.Quantity,0) as money)end 'SAPCost'*/
	, Case When ISNULL((Select OBVL.CalcPrice from OBVL with(nolock) where OBVL.AbsEntry = (Select MAX(OBVL.AbsEntry) from OBVL with(nolock) Left join OILM with(nolock) on OBVL.ILMEntry = OILM.MessageID where Case when OBVL.DocType = 162 and (Select MAX(S0.CreateDate) from OBVL S0 Where S0.ItemCode = T0.ItemCode and S0.SysNumber = T0.SysNumber and S0.DocType = 202) > OILM.DocDate then OBVL.CreateDate When OBVL.Cost = 0 then OBVL.CreateDate else OILM.DocDate end <= @date and OBVL.SysNumber = T0.SysNumber and OBVL.ItemCode = T0.ItemCode)),0)= 0 then (Case when T2.Quantity = 0 then isnull((Case when T2.CostTotal = 0 then NULL else T2.CostTotal end)/Sum(T0.Quantity),0) else isnull((Case when T2.CostTotal = 0 then NULL else T2.CostTotal end)/T2.Quantity,0) end) Else (Select OBVL.CalcPrice from OBVL with(nolock) where OBVL.AbsEntry = (Select MAX(OBVL.AbsEntry) from OBVL with(nolock) Left join OILM with(nolock) on OBVL.ILMEntry = OILM.MessageID where Case when OBVL.DocType = 162 and (Select MAX(S0.CreateDate) from OBVL S0 Where S0.ItemCode = T0.ItemCode and S0.SysNumber = T0.SysNumber and S0.DocType = 202) > OILM.DocDate then OBVL.CreateDate When OBVL.Cost = 0 then OBVL.CreateDate else OILM.DocDate end <= @date and OBVL.SysNumber = T0.SysNumber and OBVL.ItemCode = T0.ItemCode)) End  'SAPCost'
	, T6.FormatCode 'InvAccount'
	, Case when T3.QryGroup1 = 'Y' then 'RAW'
	when T3.QryGroup2 = 'Y' then 'FAB'
	when T3.QryGroup3 = 'Y' then 'EWS'
	when T3.QryGroup4 = 'Y' then 'ASSEM'
	when T3.QryGroup5 = 'Y' then 'FT'
	when T3.QryGroup6 = 'Y' then 'FG'
	when T3.QryGroup7 = 'Y' then 'FG'
	when T3.QryGroup8 = 'Y' then 'FAB'
	When T3.Qrygroup9 = 'Y' then 'EWS' end as 'SAPStage' 
	,Case when T4.ItmsGrpNam like '%Condor%' then 'Condor' else T4.ItmsGrpNam end 'Family'
	,'' 'WO#'
              , '' 'SpinwebWONo'
			  , '' 'PRDO_StartDate'
              , T2.AbsEntry
	from ITL1 T0 with(nolock)
	inner join OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry
	inner join OBTN T2 with(nolock) on T0.SysNumber = T2.SysNumber and T0.ItemCode = T2.ItemCode
	inner join OITM T3 with(nolock) on T0.ItemCode = T3.ItemCode
	inner join OITB T4 with(nolock) on T3.ItmsGrpCod = T4.ItmsGrpCod
	Inner join OITW T5 with(nolock) on T1.ItemCode = T5.ItemCode and T1.LocCode = T5.WhsCode
	Inner join OACT T6 with(nolock) on T5.BalInvntAc = T6.AcctCode
	--left join ITT1 T6 with(nolock) on T3.ItemCode = T6.Code and T3.QryGroup1 = 'Y'
	Where T1.DocDate <= @date --and T2.DistNumber = 'UEVU48000.44C'-- DATEADD(MONTH, DATEDIFF(MONTH, -1, @Date)-1, -1) --and T0.ItemCode = 'TMAY06B-EUZQ'
	Group by T2.CreateDate, T2.AbsEntry, T2.Costtotal, T2.Quantity, T5.U_StdCost, T0.ItemCode, T2.DistNumber, Cast(T2.Notes as Nvarchar), T1.LocCode, T3.QryGroup1, T3.QryGroup2, T3.QryGroup3, T3.QryGroup4, T3.QryGroup5, T3.QryGroup6, T3.QryGroup7, T3.QryGroup8, T3.QryGroup9, T4.ItmsGrpNam, T6.FormatCode, T2.LotNumber, T0.SysNumber
	Having Sum(T0.Quantity) <> 0
	
	Union all 
	-- This section is calculating the WIP On hand and WIP cost --

	-- Declare @Date date = '20210630'
	Select  Convert(date, T2.CreateDate, 112) 'WIPDateAdd'
	, T0.ItemCode 'WIPItem'
	, T2.DistNumber
	, Cast(T2.Notes as nvarchar) 'ParentLot'
    , T2.LotNumber 'BatchAtt2'
	, T1.LocCode 'Whse'
	-- This calculates the Quantity of what is on hand based on Issued to Production minus Receipt from Production with Receipt from Production Parent Lot linked to Issued to Production Lot and matching production order numbers.  It also calculates the gross die quantity for the EWS stage where wafers become die --
	, Abs(sum(distinct T0.Quantity)) - Case when T0.ItemCode = 'WA01N79C' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/1477),2,0)as int) 
		When T0.ItemCode in ('WB06M35M','WB08M35M','WB07M35M') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/4690),2,0)as int) 
		when T0.ItemCode = 'WB02N43G-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/365),2,0)as int) 
		When T0.Itemcode = 'Logan_EWS' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/607),2,0)AS int) 
		When T0.ItemCode = 'WC01N10C-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/43000),2,0)as int) 
		When T0.Itemcode = 'WA00N44A' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/430),2,0)as int) 
		When T0.ITemcode = 'MR0A16AVYS35' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/90),2,0)As Int) 
		when T0.ITemCode in ('WA05N28J', 'WA04N28J-ENG') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3927),2,0)As Int) 
		When T0.ItemCode in ('WB01N52R','WA00N52R-ENG', 'WB00N52R-ENG') then  Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/6560),2,0)As Int) 
		When T0.ItemCode = 'WC24L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/898),2,0)as int) 
		When T0.ItemCode = 'WC23L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/976),2,0)as int) 
		When T0.ItemCode = 'WA01M13Y' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3898),2,0)as Int)  
		When T0.ItemCode = 'MCKINLEY.09' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/597),2,0)as Int) 
		else Cast(sum(Distinct isnull(T4.RecvQty,0))as int) end 'WIPQty'
	-- This calculates the cost of the WIP on hand including issued Cost of the Resources (per their issued cost at the time), and aggregates the costs together.
	--, Cast((isnull((sum(distinct T9.StockPrice) *abs(sum(distinct T0.Quantity))+ (isnull((T10.ResCost/(Select SUM(OITL.DefinedQty) 'TotalIssued' from OITL WHERE OITL.StockEff = 1 and OITL.DocType = 60 and OITL.DocDate <= @date and OITL.BaseEntry = T1.BaseEntry) /*sum(T1.DefinedQty)*/),0)*abs(sum(distinct T0.Quantity)))) /abs(sum(Distinct T0.Quantity)),T7.U_StdCost)/*+(isnull((Select Sum(CompTotal) 'WB' from WOR1 with(nolock) where ItemType = 290 and ItemCode like '%Walk%Back%' and WOR1.DocEntry = T1.BaseEntry),0)/(Select SUM(OITL.DefinedQty) 'TotalIssued' from OITL WHERE OITL.StockEff = 1 and OITL.DocType = 60 and OITL.DocDate <= @date and OITL.BaseEntry = T1.BaseEntry) T1.DefinedQty)*/)-isnull(T11.StockPrice,0) as money) 'WipCost'
	, (T12.Remainder/(Select Count(Sysnumber) 'Records' from ITL1 inner join OITL on ITL1.LogEntry = OITL.LogEntry and OITL.StockEff =1 where OITL.BaseEntry = T1.BaseEntry and OITL.DocType = 60 and OITL.DocDate <= @date))/(Abs(sum(distinct T0.Quantity)) - Case when T0.ItemCode = 'WA01N79C' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/1477),2,0)as int) 
		When T0.ItemCode in ('WB06M35M','WB08M35M','WB07M35M') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/4690),2,0)as int) 
		when T0.ItemCode = 'WB02N43G-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/365),2,0)as int) 
		When T0.Itemcode = 'Logan_EWS' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/607),2,0)AS int) 
		When T0.ItemCode = 'WC01N10C-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/43000),2,0)as int) 
		When T0.Itemcode = 'WA00N44A' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/430),2,0)as int) 
		When T0.ITemcode = 'MR0A16AVYS35' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/90),2,0)As Int) 
		when T0.ITemCode in ('WA05N28J', 'WA04N28J-ENG') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3927),2,0)As Int) 
		When T0.ItemCode in ('WB01N52R','WA00N52R-ENG', 'WB00N52R-ENG') then  Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/6560),2,0)As Int) 
		When T0.ItemCode = 'WC24L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/898),2,0)as int) 
		When T0.ItemCode = 'WC23L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/976),2,0)as int) 
		When T0.ItemCode = 'WA01M13Y' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3898),2,0)as Int)  
		When T0.ItemCode = 'MCKINLEY.09' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/597),2,0)as Int) 
		else Cast(sum(Distinct isnull(T4.RecvQty,0))as int) end) 'WipCost2'
	--, T9.StockPrice
	--, T11.StockPrice
	--, T10.ResCost
	, T9.FormatCode 'WIPInvAcct'
	--, sum(distinct isnull(T4.RecvQty,0)) 'RecvQty'
	--, abs(sum(DISTINCT T0.Quantity)) 'Issued'
	--, SUM(DISTINCT T1.DefinedQty) 'IssuedtotalQty'
	, Case when T5.QryGroup1 = 'Y' then 'FAB'
	when T5.QryGroup2 = 'Y' then 'EWS'
	When T5.QryGroup3 = 'Y' then 'ASSEM'
	When T5.QryGroup4 = 'Y' then 'FT'
	When T5.QryGroup5 = 'Y' then 'FGWIP'
	When T5.QryGroup6 = 'Y' then 'FG'
	When T5.QryGroup7 ='Y' then 'FG'
	When T5.QryGroup8 = 'Y' then 'FAB'
	When T5.QryGroup9 = 'Y' then 'EWS' end 'Stage'
	, Case when T6.ItmsGrpNam like '%Condor%' then 'Condor' else T6.ItmsGrpNam end 'Family' 
	, Cast(T1.BaseEntry as varchar) 'WO#'
	, (Select OWOR.U_SpinwebNo from OWOR where OWOR.DocEntry = T1.BaseEntry) 'SpinwebWONo'
	, CAST((Select FORMAT(Convert(date,OWOR.PostDate,112),'MM/dd/yyyy') from OWOR where OWOR.DocEntry = T1.BaseEntry)as nvarchar) 'PRDO_StartDate'
    , T2.AbsEntry
		
	from ITL1 T0 with(nolock)
	inner join OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.DocType = 60 and T1.StockEff = 1
	inner join OBTN T2 with(nolock) on T0.SysNumber = T2.SysNumber and T0.ItemCode = T2.ItemCode
	-- T3 Subquery is looking for all Open Work orders and Historically open work orders --
	Inner join (-- Declare @Date date = '20210331'
				Select T0.DocEntry from OWOR T0 Where T0.Status = 'R'
				Union
				Select T0.DocEntry from AWOR T0 Inner join OWOR T1 on T0.DocEntry = T1.DocEntry where T1.CloseDate > @date Group by T0.DocEntry --Having Max(T0.UpdateDate) between dateadd(mm,-1, @date) and @date
				) T3 on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202
	-- T4 Subquery is looking for Received lots to compare agains the issued lots
	Left Join (-- Declare @Date date = '20201123'
		Select Sum(T0.Quantity) 'RecvQty', T3.DocEntry, T0.ItemCode, T2.DistNumber 'RecvLot', Cast(T2.Notes as nvarchar) 'RecvPLot'
		from ITL1 T0 with(nolock) 
		inner join OITL T1 with(Nolock) on T0.LogEntry = T1.LogEntry and T1.DocType = 59 and T1.DocDate <= @date
		inner join OBTN T2 with(nolock) on T0.SysNumber = T2.SysNumber and T0.ItemCode = T2.ItemCode 
		inner join (-- Declare @Date date = '20200731'
					Select T0.DocEntry from OWOR T0 Where T0.Status = 'R'
					Union
					Select T0.DocEntry from AWOR T0 Inner join OWOR T1 on T0.DocEntry = T1.DocEntry where T1.CloseDate > @date Group by T0.DocEntry --Having Max(T0.UpdateDate) between dateadd(mm,-1, @date) and @date
					)T3 on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202
		group by T3.DocEntry, T0.ItemCode, T2.DistNumber, Cast(T2.Notes as nvarchar)
		)T4 on T1.BaseEntry = T4.DocEntry and T2.DistNumber = T4.RecvPLot
	Left Join OITM T5 with(nolock) on T0.ItemCode = T5.ItemCode
	Left Join OITB T6 with(noLock) on T5.ItmsGrpCod = T6.ItmsGrpCod
	Left Join OITW T7 with(nolock) on T1.ItemCode = T7.ItemCode and T1.LocCode = T7.WhsCode
	-- T8 subQuery is looking for Planned quantity of the WOR1 table of the Open T3 Production Orders
	left join (select DocEntry, PlannedQty from WOR1 with(nolock) Where ItemType = 4) T8 on T3.DocEntry = T8.DocEntry
	-- T9 subquery is looking for the issued quantity and cost of Issue of Production matched to the OITL transaction table
	left join (Select DocEntry, LineNum, OACT.FormatCode, StockPrice, ItemType/*, Quantity 'IssuedQty'*/ from IGE1 with(nolock) inner join OACT with(nolock) on IGE1.AcctCode = OACT.AcctCode where ItemType = 4 and IGE1.DocDate <= @date) T9 on T1.DocEntry = T9.DocEntry and T1.DocLine = T9.LineNum
	-- T10 subquery is looking for Resource cost from Issued of production related to OITL transaction table
	left join (Select BaseEntry, sum(StockPrice * Quantity) 'ResCost' from IGE1 with(nolock) where ItemType=290 and IGE1.DocDate <= @date Group by BaseEntry) T10 on T1.BaseEntry = T10.BaseEntry
	-- T11 Subquery is looking for the Cost and Quantity of Receipt from Prdocution related to the Production order from the OITL transaction table
	left join (-- Declare @Date date = '20210331'
		Select IGN1.BaseEntry, IGN1.StockPrice, IGN1.Itemtype, IGN1.ItemCode /*, IGN1.Quantity*/ from IGN1 with(nolock) inner join OIGN on IGN1.DocEntry = OIGN.DocEntry WHERE IGN1.DocDate <=@date and IGN1.WhsCode <> 'T_UTC_R')T11 on T1.ItemCode = T11.ItemCode and T11.BaseEntry = T1.BaseEntry
	-- Subquery to get total issued cost, minus total recieved cost, which would return the remaning cost in WIP
	left Join (Select SUM(S2.IssuedCost) - Sum(ISNULL(S3.Recvcost,0)) 'Remainder', S2.BaseEntry
			from(
				Select sum(IssuedCost) 'IssuedCost', BaseEntry
				From(
					Select ROUND(SUM(StockPrice)*Quantity,2,2) 'IssuedCost', BaseEntry
					From IGE1
					Where IGE1.DocDate <= @date -- and BaseEntry = 17285
					Group by Quantity, BaseEntry
					)S0	
				Group by BaseEntry					
				)S2
			Left Join (Select Sum(Recvcost) 'RecvCost', BaseEntry
				From(
					Select ROUND(SUM(StockPrice)*Quantity,2,2) 'Recvcost', BaseEntry
					From IGN1
					Where IGN1.DocDate <= @date --and  BaseEntry = 17285
					Group by Quantity, BaseEntry
					)S1
				Group By BaseEntry
				)S3 on S2.BaseEntry = S3.BaseEntry
			Group by S2.BaseEntry
			)T12 on T1.BaseEntry = T12.BaseEntry
	Where T1.DocDate <= @date --and T1.BaseEntry = 17285 --and DistNumber = 'TZ60598.1X$4'
	Group by T0.ItemCode, T5.QryGroup1, T5.QryGroup2, T5.QryGroup3, T5.Qrygroup4, T5.QryGroup5, T5.QryGroup6, T5.QryGroup7, T5.Qrygroup8, T5.Qrygroup9, T6.ItmsGrpNam, T2.DistNumber, Cast(T2.Notes as nvarchar), T1.LocCode, T1.BaseEntry, T2.CreateDate, T7.U_StdCost, T10.ResCost, /*T12.TotalIssued, T1.DefinedQty,*/ T9.FormatCode, T2.LotNumber, T2.AbsEntry, T11.StockPrice, T12.Remainder--, T11.Quantity 
	--, T9.StockPrice, T10.ResCost
	Having Abs(sum(distinct T0.Quantity)) - Case when T0.ItemCode = 'WA01N79C' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/1477),2,0)as int) 
		When T0.ItemCode in ('WB06M35M','WB08M35M','WB07M35M') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/4690),2,0)as int) 
		when T0.ItemCode = 'WB02N43G-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/365),2,0)as int) 
		When T0.Itemcode = 'Logan_EWS' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/607),2,0)AS int) 
		When T0.ItemCode = 'WC01N10C-ENG' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/43000),2,0)as int) 
		When T0.Itemcode = 'WA00N44A' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/430),2,0)as int) 
		When T0.ITemcode = 'MR0A16AVYS35' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/90),2,0)As Int) 
		when T0.ITemCode in ('WA05N28J', 'WA04N28J-ENG') then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3927),2,0)As Int) 
		When T0.ItemCode in ('WB01N52R','WA00N52R-ENG', 'WB00N52R-ENG') then  Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/6560),2,0)As Int) 
		When T0.ItemCode = 'WC24L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/898),2,0)as int) 
		When T0.ItemCode = 'WC23L72W' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/976),2,0)as int) 
		When T0.ItemCode = 'WA01M13Y' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/3898),2,0)as Int)  
		When T0.ItemCode = 'MCKINLEY.09' then Cast(Round(sum(Distinct isnull(T4.RecvQty,0)/597),2,0)as Int) 
		else Cast(sum(Distinct isnull(T4.RecvQty,0))as int) end <> 0
	)T0
)T0
--Where T0.PerUnitLotCost < 10000.00 --and T0.WO# = 16787
Order by Case When Stage = 'FAB' then 1 when Stage = 'EWS' then 2 when Stage = 'ASSEM' then 3 when Stage = 'FT' then 4 when Stage = 'FG' then 5 end ASC, Family, ItemCode