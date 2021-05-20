Declare @AsOfDate Date
Declare @Booking numeric(19,6)
Declare @Backlog numeric(19,6)
Declare @ShipTotal numeric(19,6)
Declare @ShipToggle numeric(19,6)
Declare @shipSensor numeric(19,6)
Declare @ShipEmbed numeric(19,6)
Declare @ShipSTTMRAM numeric(19,6)
Declare @SD date
Declare @ED Date
Declare @CurDate date
Declare @Code int
Declare @Name int
Declare @NetAdjustments numeric(19,6)
Declare @BacklogShip numeric(19,6)
Declare @TotalRev numeric(19,6)
Declare @count as Date
Declare @QtrDays as int
Declare @CountStart as Float
Declare @YearQtr as nvarchar(6)
Declare @MidpiontGuide as int


-------------- Set Days, linearity, Midpoint Guidance and Qtr reference ------------------------------
	Set @SD = dateadd(qq, datediff(qq, 0, getdate()),0)   --First day of current quarter
	Set @ED = dateadd(dd, -1, dateadd(qq, datediff(qq,0,getdate())+1, 0)) -- Last day of current quarter
	set @count = @sd
	Set @QtrDays = datediff(dd, @sd, @Ed)+1
	Set @countStart = 1.00
	set @YearQtr = Cast(left(convert(date, getdate(),112),4)as nvarchar) + 'Q'+ cast(datepart(quarter,getdate())as nvarchar)
	Set @Code = (Select isnull(max(cast(Code as int))+1,1) from dbo.[@SHIP_LINEARITY])
	Set @Name = (Select isnull(max(cast(name as int))+1,1) from dbo.[@SHIP_LINEARITY])
	Set @MidpiontGuide = (Select isnull(U_MidpointGuide,11000000) from dbo.[@SHIP_LIN_MANUAL] where U_AsOfDate = dateadd(dd, -1, convert(date, getdate(), 112)))

If Not Exists(Select Top 1 U_YearQtr from dbo.[@SHIP_LINEARITY] where U_YearQtr = @YearQtr)

Begin
	While @count <=@ED
	Begin
		Insert Into dbo.[@SHIP_LINEARITY] (Code, Name, U_YearQtr, U_Day, U_AsOfDate, U_Linearity, U_MidpointGuide) values (@code, @Name, @YearQtr, @CountStart, @count, round(Cast((@CountStart/@QtrDays)*100 as float),0,0), round(cast((@CountStart/@QtrDays)*@MidpiontGuide as float),2,2))
		Set @count = dateadd(dd,1,@count)
		Set @CountStart = @CountStart + 1
		Set @code = @code + 1
		Set @Name = @name + 1
	end
end


----------------  Populate The Bookings Total   ----------------------------------------------

Begin

	Set @SD = dateadd(qq, datediff(qq, 0, getdate()),0)   --First day of current quarter
	Set @ED = dateadd(dd, -1, dateadd(qq, datediff(qq,0,getdate())+1, 0)) -- Last day of current quarter
	Set @CurDate =  convert(date, getdate(),112) -- Current date

	Select @Booking = sum(isnull(T0.[Toggle Bookings Total],0))+sum(isnull(t0.[STTMRAM Bookings Total],0))+sum(isnull(T0.[Embedded Bookings Total],0))+sum(isnull(T0.[Sensor Bookings Total],0))
	From
	(Select Case when T3.U_familygroup = 'Toggle' then sum(T1.Quantity*T1.Price) end as 'Toggle Bookings Total'
	, Case when T3.U_FamilyGroup = 'Sensors' then Sum(T1.Quantity*T1.Price) end as 'Sensor Bookings Total'
	, Case when T3.U_FamilyGroup in ('Systems', 'Spin Torque') then Sum(T1.Quantity*T1.Price) end as 'STTMRAM Bookings Total'
	, Case when T3.U_FamilyGroup in ('Embedded', 'Other') then sum(T1.Quantity*T1.Price) end as 'Embedded Bookings Total'
	, T1.U_SupPromShipDate, T0.DocDate
	from EverspinTech.dbo.ORDR T0 
	INNER JOIN EverspinTech.dbo.RDR1 T1 on T0.DocEntry = T1.DocEntry
	INNER JOIN EverspinTech.dbo.OITM T2 on T1.ItemCode = T2.ItemCode
	inner join EverspinTech.dbo.OITB T3 on T2.ItmsGrpCod = T3.ItmsGrpCod
	Where T1.U_SupPromShipDate between @SD and @ED and T1.U_LineStatus <>'C'
	group by T1.U_SupPromShipDate, T3.U_FamilyGroup, T0.DocDate) T0
	Where T0.DocDate <= @CurDate

	UPDATE dbo.[@SHIP_LINEARITY] Set U_Booking = @Booking where U_AsOfDate = @CurDate
end


----------------  Populate the Shipment Totals ----------------------------------------------

Begin
	Set @SD = dateadd(qq, datediff(qq, 0, getdate()),0)   --First day of current quarter
	Set @CurDate = convert(date, getdate(),112) -- Current date

	Select @shipTotal = sum(isnull(T0.ToggleTotal,0))+sum(isnull(T0.SensorTotal,0))+sum(isnull(T0.STTMRAMTotal,0))+sum(isnull(T0.EmbeddedTotal,0)), @ShipToggle = sum(isnull(T0.ToggleTotal,0)), @shipSensor = Sum(isnull(T0.SensorTotal,0)), @ShipSTTMRAM = sum(isnull(T0.STTMRAMTotal,0)), @ShipEmbed = Sum(isnull(T0.EmbeddedTotal,0))
	from(
	Select Case when T3.U_familygroup = 'Toggle' then sum(T1.Quantity*T1.Price) end as 'ToggleTotal'
	, Case when T3.U_FamilyGroup = 'Sensors' then Sum(T1.Quantity*T1.Price) end as 'SensorTotal'
	, Case when T3.U_FamilyGroup in ('Systems', 'Spin Torque') then Sum(T1.Quantity*T1.Price) end as 'STTMRAMTotal'
	, Case when T3.U_FamilyGroup in ('Embedded', 'Other') and T5.Docnum <> 10455  then sum(T1.Quantity*T1.Price) end as 'EmbeddedTotal'
	, T1.U_ActualShipDate
	from EverspinTech.dbo.ORDR T0 
	INNER JOIN EverspinTech.dbo.RDR1 T1 on T0.DocEntry = T1.DocEntry
	INNER JOIN EverspinTech.dbo.OITM T2 on T1.ItemCode = T2.ItemCode
	inner join EverspinTech.dbo.OITB T3 on T2.ItmsGrpCod = T3.ItmsGrpCod
	left join EverspinTech.dbo.DLN1 T4 on T1.DocEntry = T4.BaseEntry and T1.LineNum = T4.BaseLine
	left join EverspinTech.dbo.ODLN T5 on T4.DocEntry = T5.DocEntry
	Where T1.U_ActualShipDate between @SD and @CurDate and T1.U_LineStatus <> 'C' 
	Group By  T1.U_ActualShipDate, T3.U_FamilyGroup,T5.DocNum) T0
	

	Update dbo.[@SHIP_LINEARITY] Set U_ShipmentTotal = @shiptotal, U_ShipToggle = @ShipToggle, U_ShipSensor = @shipSensor, U_ShipSTTMRAM = @ShipSTTMRAM, U_ShipEmbedded = @ShipEmbed where U_AsofDate = @CurDate
End

----------- Populate Backlog Total ------------------------------------------------

Begin
	
	Set @ED = dateadd(dd, -1, dateadd(qq, datediff(qq,0,getdate())+1, 0)) -- Last day of current quarter
	Set @CurDate = convert(date, getdate(),112) -- Current date
	
	Select @Backlog = sum(isnull(T0.ToggleBacklogTotal,0))+sum(isnull(T0.SensorBacklogTotal,0))+sum(isnull(STTMRAMBacklogTotal,0))+sum(isnull(T0.EmbeddedBacklogTotal,0))
	from(
	Select Case when T3.U_familygroup = 'Toggle' then sum(T1.Quantity*T1.Price) end as 'ToggleBacklogTotal'
	, Case when T3.U_FamilyGroup = 'Sensors' then Sum(T1.Quantity*T1.Price) end as 'SensorBacklogTotal'
	, Case when T3.U_FamilyGroup in ('Systems', 'Spin Torque') then Sum(T1.Quantity*T1.Price) end as 'STTMRAMBacklogTotal'
	, Case when T3.U_FamilyGroup in ('Embedded', 'Other') and T0.DocNum = 38971 then '142307.69' when T3.U_FamilyGroup in ('Embedded', 'Other') then sum(T1.Quantity*T1.Price) end as 'EmbeddedBacklogTotal'
	, T1.U_SupPromShipDate, T0.DocDate
	from EverspinTech.dbo.ORDR T0
	Inner Join EverspinTech.dbo.RDR1 T1 on T0.DocEntry = T1.DocEntry
	INNER JOIN EverspinTech.dbo.OITM T2 on T1.ItemCode = T2.ItemCode
	inner join EverspinTech.dbo.OITB T3 on T2.ItmsGrpCod = T3.ItmsGrpCod
	where T1.LineStatus = 'O' and T1.U_SupPromShipDate <= @ED and isnull(T1.U_ActualShipDate,'') = ''
	Group by T1.U_SupPromShipDate, T3.U_FamilyGroup, T0.DocDate, T0.DocNum) T0
	Where T0.DocDate <= @CurDate

	update dbo.[@SHIP_LINEARITY] Set U_Backlog = @Backlog where U_AsOfDate = @CurDate

End

-------------- Populate Net Adjustments and Backlog + Shipment totals ---------------------------------

Begin
	
	Set @CurDate = convert(date, getdate(),112) -- Current date

	Select @NetAdjustments = T0.U_ShipToggle * ISNULL(T1.U_NetAdjustRate,'0.05'), @BacklogShip = T0.U_Backlog + T0.U_ShipmentTotal, @TotalRev = (T0.U_ShipmentTotal-(T0.U_ShipToggle * ISNULL(T1.U_NetAdjustRate,'0.05')))+ ISNULL(T1.U_RoyaltyAndNRE,'0.00')
	from dbo.[@SHIP_LINEARITY] T0
	LEFT join dbo.[@SHIP_LIN_MANUAL] T1 on T0.U_AsOfDate = T1.U_AsOfDate
	Where T0.U_AsOfDate = @CurDate

	Update dbo.[@SHIP_LINEARITY] Set U_NetAdjustments = @NetAdjustments, U_BacklogShipment = @BacklogShip, U_TotalRevenue = @TotalRev Where U_AsOfDate = @CurDate

End