/*
Types of slowly changing dimensions

Type 1 SCD - overwrite old value, have a column to track modifyingdate

Type 2 SCD - Add new row with changed value, have a colum to track IsCurrent, EndDate

Type 3 SCD - stores two versions of a dimension in separate columns - CurrentValue, 
Original Value, ModifiedDate
*/

