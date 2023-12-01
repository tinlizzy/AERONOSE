/****************************************************************************/
/******************  AERONOSE Project    					*****************/
/******************  AP exposure window construction  		*****************/
/******************  Combined coding for PM2.5 & NO2/O3		*****************/

/******************  Created: 		11/8/2023 				*****************/
/******************  Last updated: 	11/21/2023 				*****************/
/******************  Author: Tara Jenson					*****************/
/****************************************************************************/

/* includes these steps: */
/* 1) subset RADC data to a) MAP cohort only, b) with age of death (autopsied) */
/* 2) merge MAP data with AP data (2 sets of data: one with no2/o3 exps, one with pm2.5 ) */
/* 3) code the following dates: */
/*	--> a) date of death: death_yr-MM-DD */
/*  --> b) start date of 7 year exp window: date of death - 7 yrs (i.e. 07-01-xxxx) */
/*  --> c) end date of 7 year exp window: 1 day prior to DoD (i.e. 06-30-xxxx) */
/* 4) subset datasets to just those inclusive of the 7 yr exp window */
/* 	--> no2/o3: obs with exp_7yr_yyyymmdd GE 1990-07-01 and exp_end_yyyymmdd LE 2019-06-30 */
/* 	--> pm25: obs with exp_7yr_yyyymmdd GE 1999-07-01 and exp_end_yyyymmdd LE 2017-06-30 */

/** set SAS library location **/
libname AERONOSE '/home/tarajenson0/sasuser.v94/BU/AERONOSE/data';

/* 1) subset RADC data to those with age of death (autopsied) */
proc contents data = AERONOSE.datasetBasicPlusBaselineYr_v5; run;

proc sql;
select count(*) as N_obs /* no2/o3 3-yr exp windows */
from AERONOSE.datasetBasicPlusBaselineYr_v5
where study like "MAP" and age_death NE .; 
quit; /* 1310 */

proc freq data = AERONOSE.datasetBasicPlusBaselineYr_v5;
	tables study*died_temp /missing ; /* died_temp created elsewhere based on having age of death */
	run; /* 1310 for MAP with died status */
	
data AERONOSE.died_maponly (keep=projid_num study age_death died_temp birth_yr death_yr);
	set AERONOSE.datasetBasicPlusBaselineYr_v5;
	where died_temp and study like "MAP";
	run; /* 1310 */
	
data AERONOSE.died_maponly (rename=(projid_num = projid));
	set AERONOSE.died_maponly;
	run;

proc contents data = AERONOSE.died_maponly; run;

proc sql;
select count(*) as N_obs 
from AERONOSE.died_maponly
where death_yr GT 2017; 
quit; 

proc sql;
select count(*) as N_obs 
from AERONOSE.datasetBasicPlusBaselineYr_v5
where death_yr GT 2017 and study like "MAP"; 
quit;

/* 2) merge MAP data with AP data (2 sets of data: one with no2/o3 exps, one with pm2.5 ) */
/* sort maponly & AP files by projid for merging */
proc sort data = AERONOSE.died_maponly; by projid; run;
proc sort data = AERONOSE.dr1010_no2; by projid; run; /* orig no2 long file */
proc sort data = AERONOSE.dr1010_o3; by projid; run;	/* orig o3 long file */
proc sort data = AERONOSE.dr1010_pm25; by projid; run;	/* orig pm25 long file */

proc contents data = AERONOSE.dr1010_no2; run;

data AERONOSE.died_maponly_no2_o3;
	merge AERONOSE.died_maponly (in=a) AERONOSE.dr1010_no2 (in=b);
	by projid; /* merge MAP/died & no2 data - only if in both (i.e. MAP/died) */
	if a and b;
	run;
	
proc contents data = AERONOSE.died_maponly_no2_o3; run; /* 1,024,947 */

data AERONOSE.died_maponly_no2_o3 (rename=(start = no2_start end = no2_end tie_addr = no2_tie_addr));
	set AERONOSE.died_maponly_no2_o3; /* rename AP vars to no2 specific */
	run;
	
data AERONOSE.died_maponly_no2_o3;
	merge AERONOSE.died_maponly_no2_o3 (in=a) AERONOSE.dr1010_o3 (in=b);
	by projid; /* merge in o3 data - only if in both (i.e. MAP/died) */
	if a and b;
	run; /* 1,024,947 */
	
data AERONOSE.died_maponly_no2_o3 (rename=(start = o3_start end = o3_end tie_addr = o3_tie_addr));
	set AERONOSE.died_maponly_no2_o3; /* rename new AP vars to o3 specific */
	run;

proc contents data = AERONOSE.died_maponly_no2_o3; run;

data AERONOSE.died_maponly_no2_o3 (drop=death_yr_GT2017 death_yr_GT2019 death_yr_LT2006);
	set AERONOSE.died_maponly_no2_o3;
	run;

data AERONOSE.died_maponly_pm25; /* separate dataset for pm25 since date ranges different */
	merge AERONOSE.died_maponly (in=a) AERONOSE.dr1010_pm25 (in=b);
	by projid;
	if a and b;
	run; /* 632,247 */
	
proc contents data = AERONOSE.died_maponly_pm25; run; /* 632,247 */
proc print data = AERONOSE.died_maponly_pm25 (obs=25); run;

data AERONOSE.died_maponly_pm25 (rename=(start = pm25_start end = pm25_end tie_addr = pm25_tie_addr));
	set AERONOSE.died_maponly_pm25; /* rename AP vars to pm25 specific */
	run; /* 632,247 */
	
data AERONOSE.died_maponly_pm25 (drop=death_yr_GT2017 death_yr_LT2006);
	set AERONOSE.died_maponly_pm25;
	run; 

/* 3) code the following dates: */
/*	a) --> date of death: death_yr-MM-DD */
/*  b) --> start date of 7 year exp window: date of death - 7 yrs (i.e. 07-01) */
/*  c) --> end date of 7 year exp window: 1 day prior to DoD (i.e. 06-30) */

/* for no2 & o3 dataset */
/*	a) --> date of death: death_yr-MM-DD */
data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3;
	death_yr_char = put(death_yr, 10.);
	run; /* convert year of death numeric to char var */
	
proc contents data = AERONOSE.died_maponly_no2_o3_v2; run;
proc print data = AERONOSE.died_maponly_no2_o3_v2 (obs=20); run;

data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3_v2;
	death_yyyymmdd_char = compress(trim(death_yr_char)||"-07-01");
	run; /* append -07-01 to year of death, still char string, remove blank spaces */

data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3_v2;
	death_yyyymmdd = input(death_yyyymmdd_char,yymmdd10.);
	format death_yyyymmdd yymmdd10.;
	run; /* convert to date num, then format as yyyy-mm-dd */

/*  b) --> start date of 7 year exp window: date of death - 7 yrs (i.e. 07-01) */
proc contents data = AERONOSE.died_maponly_no2_o3_v2; run;
proc print data = AERONOSE.died_maponly_no2_o3_v2 (obs=20); run;

data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3_v2;
 	exp_7yr_start_year = death_yr - 7; /* combined everything into 1 data step */
 	exp_7yr_start_yr_ch = put(exp_7yr_start_year, 10.);
 	exp_7yr_yyyymmdd_ch = compress(trim(exp_7yr_start_yr_ch)||"-07-01");
 	exp_7yr_yyyymmdd = input(exp_7yr_yyyymmdd_ch,yymmdd10.);
	format exp_7yr_yyyymmdd yymmdd10.;
 	run;

/*  c) --> end date of 7 year exp window: 1 day prior to DoD (i.e. 06-30) */
data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3_v2;
  	exp_end_yyyymmdd_ch = compress(trim(death_yr_char)||"-06-30");
 	exp_end_yyyymmdd = input(exp_end_yyyymmdd_ch,yymmdd10.);
	format exp_end_yyyymmdd yymmdd10.;
 	run;

/* for pm25 dataset */
/*	a) --> date of death: death_yr-MM-DD */
proc contents data = AERONOSE.died_maponly_pm25; run;
proc print data = AERONOSE.died_maponly_pm25 (obs=20); run;
data AERONOSE.died_maponly_pm25_v2;
	set AERONOSE.died_maponly_pm25;
	death_yr_char = put(death_yr, 10.);
	death_yyyymmdd_char = compress(trim(death_yr_char)||"-07-01");
	death_yyyymmdd = input(death_yyyymmdd_char,yymmdd10.);
	format death_yyyymmdd yymmdd10.;
	run; /* convert year of death numeric to char var */

/*  b) --> start date of 7 year exp window: date of death - 7 yrs (i.e. 07-01) */
proc contents data = AERONOSE.died_maponly_pm25_v2; run;
proc print data = AERONOSE.died_maponly_pm25_v2(obs=20); run;

data AERONOSE.died_maponly_pm25_v2;
	set AERONOSE.died_maponly_pm25_v2;
 	exp_7yr_start_year = death_yr - 7; 
 	exp_7yr_start_yr_ch = put(exp_7yr_start_year, 10.);
 	exp_7yr_yyyymmdd_ch = compress(trim(exp_7yr_start_yr_ch)||"-07-01");
 	exp_7yr_yyyymmdd = input(exp_7yr_yyyymmdd_ch,yymmdd10.);
	format exp_7yr_yyyymmdd yymmdd10.;
 	run;
 	
/*  c) --> end date of 7 year exp window: 1 day prior to DoD (i.e. 06-30) */
data AERONOSE.died_maponly_pm25_v2;
	set AERONOSE.died_maponly_pm25_v2;
  	exp_end_yyyymmdd_ch = compress(trim(death_yr_char)||"-06-30");
 	exp_end_yyyymmdd = input(exp_end_yyyymmdd_ch,yymmdd10.);
	format exp_end_yyyymmdd yymmdd10.;
 	run;	
 	
/* get rid of transition vars */
data AERONOSE.died_maponly_no2_o3_v2 (drop = death_yr_char death_yr_int death_yyyymmdd_char 
	exp_7yr_start_year exp_7yr_start_yr_ch exp_7yr_yyyymmdd_ch exp_end_yyyymmdd_ch);
	set AERONOSE.died_maponly_no2_o3_v2;
	run;
	
data AERONOSE.died_maponly_pm25_v2 (drop = death_yr_char death_yr_int death_yyyymmdd_char 
	exp_7yr_start_year exp_7yr_start_yr_ch exp_7yr_yyyymmdd_ch exp_end_yyyymmdd_ch);
	set AERONOSE.died_maponly_pm25_v2;
	run;
proc contents data = AERONOSE.died_maponly_no2_o3_v2; run;
proc print data = AERONOSE.died_maponly_no2_o3_v2 (obs=20); run;
proc contents data = AERONOSE.died_maponly_pm25_v2; run;
proc print data = AERONOSE.died_maponly_pm25_v2 (obs=20); run;

data AERONOSE.died_maponly_no2_o3_v2;
	set AERONOSE.died_maponly_no2_o3_v2;
	exp_7yr_start = exp_7yr_yyyymmdd; 
	format exp_7yr_start d.; /* force format to basic SAS date for easier comparison */
	exp_7yr_end = exp_end_yyyymmdd ;
	format exp_7yr_end d.;
	run;
	
data AERONOSE.died_maponly_pm25_v2;
	set AERONOSE.died_maponly_pm25_v2;
	exp_7yr_start = exp_7yr_yyyymmdd; 
	format exp_7yr_start d.; /* force format to basic SAS date for easier comparison */
	exp_7yr_end = exp_end_yyyymmdd ;
	format exp_7yr_end d.;
	run;
	
/* 4) subset datasets to just those inclusive of the 7 yr exp window */
/* 	--> no2/o3: obs with exp_7yr_yyyymmdd GE 1990-07-01 and exp_end_yyyymmdd LE 2019-06-30 */
/* 	--> pm25: obs with exp_7yr_yyyymmdd GE 1999-07-01 and exp_end_yyyymmdd LE 2017-06-30 */

/* first do some eyeballing of counts of those in the date ranges, */
/*		ranges of obs included, and that dates behaving as expected */
proc sql;
select count(*) as N_obs 
from AERONOSE.died_maponly_no2_o3_v2
where exp_7yr_start GE 11139 and exp_7yr_end LE 21730 ; 
quit; /* 822150 with 7 yr exp window start date >= July 1 1990 
			AND 7 yr exp window end date <= Jun 30 2019 */

proc sql;
select count(*) as N_obs 
from AERONOSE.died_maponly_no2_o3_v2
where exp_7yr_start LT 11139 or exp_7yr_end GT 21730 ; 
quit; /* 202797 (total of 1024947) */

proc sql;
select count(*) as N_obs 
from AERONOSE.died_maponly_pm25_v2
where exp_7yr_start GE 14426 and exp_7yr_end LE 21000 ; 
quit; /* 363699 */

proc sql;
select count(*) as N_obs 
from AERONOSE.died_maponly_pm25_v2
where exp_7yr_start LT 14426 or exp_7yr_end GT 21000 ; 
quit; /* 268548  (total of 632247) */

data AERONOSE.died_maponly_no2_o3_7y;
	set AERONOSE.died_maponly_no2_o3_v2;
	where exp_7yr_start GE 11139 and exp_7yr_end LE 21730 ;
	run; /* 822150 */
	
proc univariate data = AERONOSE.died_maponly_no2_o3_7y;
	var exp_7yr_start exp_7yr_end ;
	run; /* earliest window start date = 01 jul 1991 so check rounding of death yr */
	
proc univariate data = AERONOSE.died_maponly_no2_o3_v2;
	var death_yr ;
	run; /* yep - this tracks as the lowest YoD is 1997.64 */

data AERONOSE.died_maponly_pm25_7y;
	set AERONOSE.died_maponly_pm25_v2;
	where exp_7yr_start GE 14426 and exp_7yr_end LE 21000 ;
	run; /* 363699 */
	
proc univariate data = AERONOSE.died_maponly_pm25_7y;
	var exp_7yr_start exp_7yr_end;
	run;	
	
proc print data = AERONOSE.died_maponly_pm25_7y (obs=20); run;
	proc contents data = AERONOSE.died_maponly_pm25_7y ; run;
	
proc univariate data = AERONOSE.died_maponly_pm25_7y;
	var pm25_start pm25_end exp_7yr_start exp_7yr_end exp_7yr_yyyymmdd exp_end_yyyymmdd;
	run;

/* pm25: code indicator var for rows in the 7-yr exp window */
/* use group by and date compares */
proc sort data = AERONOSE.died_maponly_pm25_7y; by projid pm25_start; run;
data AERONOSE.died_maponly_pm25_7y_v2;
 	set AERONOSE.died_maponly_pm25_7y;
 	by projid;
 	sevenYrWin_indic = .;
 	if pm25_end LT exp_7yr_start then sevenYrWin_indic = 0; /* exclude row as < window start, move on to next row */
	else /* pm25_end GE exp_7yr_start */ do;
		if pm25_start LE exp_7yr_start then sevenYrWin_indic = 1; /* 1st row of exp win, include row, move on to next row */
		else /* pm25_start GT exp_7yr_start --> now compare exp_7yr_end */ do;
			if  pm25_end LE exp_7yr_end then sevenYrWin_indic = 1; /* middle included rows */
			else /* pm25_end GT exp_7yr_end */ do;
				if pm25_start LE exp_7yr_end then sevenYrWin_indic = 1; /* last included row of the window */
				else /* pm25_start GT exp_7yr_end */ sevenYrWin_indic = 0; /* exclude row as whole date range > end of window */
 				end;
 			end;
 		end;
 	run;
 	
proc print data = AERONOSE.died_maponly_pm25_7y_v2 (firstobs=200 obs=450); run; /* indic counts for first projid look good */
proc print data = AERONOSE.died_maponly_pm25_7y_v2 (firstobs=450 obs=600); run; /* transition to new id looks good */
proc print data = AERONOSE.died_maponly_pm25_7y_v2 (firstobs=750 obs=1000); run; /* next id counts look good */

/* now to by just those with exp win indicator */
data AERONOSE.died_map_pm25_7yrwins_only;
	set AERONOSE.died_maponly_pm25_7y_v2;
	where sevenYrWin_indic;
	run; /* 138181 --> reasonable for 753 individs with ~ 7 yrs of exp, or approx ~182 rows */
	
proc print data = AERONOSE.died_map_pm25_7yrwins_only (firstobs=185 obs=385); run;

/* no2/o3: code indicator var for rows in the 7-yr exp window (covers both no2 & o2) */
proc contents data = AERONOSE.died_maponly_no2_o3_7y; run;
proc sort data = AERONOSE.died_maponly_no2_o3_7y; by projid no2_start; run;
data AERONOSE.died_maponly_no2_o3_7y;
 	set AERONOSE.died_maponly_no2_o3_7y;
 	by projid;
 	sevenYrWin_indic = .;   /* code indicator for whether row is in the 7-yr window */
 	if no2_end LT exp_7yr_start then sevenYrWin_indic = 0; /* exclude row as < window start, move on to next row */
	else /* no2_end GE exp_7yr_start */ do;
		if no2_start LE exp_7yr_start then sevenYrWin_indic = 1; /* 1st row of exp win, include row, move on to next row */
		else /* no2_start GT exp_7yr_start --> now compare exp_7yr_end */ do;
			if  no2_end LE exp_7yr_end then sevenYrWin_indic = 1; /* middle included rows */
			else /* no2_end GT exp_7yr_end */ do;
				if no2_start LE exp_7yr_end then sevenYrWin_indic = 1; /* last included row of the window */
				else /* no2_start GT exp_7yr_end */ sevenYrWin_indic = 0; /* exclude row as whole date range > end of window */
 				end;
 			end;
 		end;
 	run;

/* spot check the indicator coding */
proc print data = AERONOSE.died_maponly_no2_o3_7y (firstobs=200 obs=450); run; /* indic counts for first projid look good */
proc print data = AERONOSE.died_maponly_no2_o3_7y (firstobs=450 obs=750); run; /* transition to new id looks good */
proc print data = AERONOSE.died_maponly_no2_o3_7y (firstobs=1400 obs=1600); run; /* next id counts look good */

/* subset by just those with exp win indicator */
data AERONOSE.died_map_no2_o3_7yrwins_only;
	set AERONOSE.died_maponly_no2_o3_7y;
	where sevenYrWin_indic;
	run; /* 192641 --> reasonable for 1050 individs with ~ 7 yrs of exp, or approx ~182 rows */
	
proc print data = AERONOSE.died_map_no2_o3_7yrwins_only (firstobs=185 obs=385); run;


/* export the no2/o3 and pm25 maponly datasets to take over to R for intervalavg */
proc export data=AERONOSE.died_maponly_pm25_7ywins_only
	outfile='/home/tarajenson0/sasuser.v94/BU/AERONOSE/data/died_maponly_pm25_7ywins_only.csv'  
	dbms=csv;
	run;

proc export data=AERONOSE.died_maponly_no2_o3_7ywins_only
	outfile='/home/tarajenson0/sasuser.v94/BU/AERONOSE/data/died_maponly_no2_o3_7ywins_only.csv'  
	dbms=csv;
	run;

proc export data=AERONOSE.died_map_pm25_7yrwins_only
	outfile='/home/tarajenson0/sasuser.v94/BU/AERONOSE/data/died_map_pm25_7yrwins_only.csv'  
	dbms=csv;
	run;

