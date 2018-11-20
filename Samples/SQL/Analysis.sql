
ALTER PROCEDURE [dbo].[DC_Typediscovery]
	@TblName nvarchar(250)
	,@fixed_id nvarchar(1000)=''
	,@getbyProductType bit=0
	,@test_ids nvarchar(1000)=''
	,@exec_level nvarchar(200)=''  /*rem_acc,acc*/
	,@testPN nvarchar(4000)=''
     ,@runstate int =1

AS
BEGIN
	SET NOCOUNT ON;

	declare @sep_level1 nvarchar(10) = ' ; '
	declare @sep_level2 nvarchar(10) = ' | '

	if @runstate in (3,4)
	  begin
	   	set @sep_level1 = N' ? '
		set @sep_level2 = N' ?? '
	  end  

	declare @PERFORMANCE_ID int


  declare @oname nvarchar(4000)
    set @oname=object_name(@@procid)
  
    declare @table_id int,@test_id int, @rowsCount int
    select @table_id=ID,@rowsCount=RowsCount  from dp_Tables where tablename='['+@TblName+']' 
    select @test_id=ID from dp_Tests where  StorProcedure =@oname

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Initialization'


     declare @uidxname varchar(64) 
     declare @uidxname1 varchar(64) 
     declare @uidxname2 varchar(64) 
     
     declare @comparableClass_id int=  dbo.fn_GetConfigValue('CATEGORIZATION_TEMPLATE_CLASS') -- 2900

    declare @pnField nvarchar(150),@pnFldId int
    select  @pnField=ColumnName,@pnFldId=id  from dp_TablesColumns where Table_ID =@table_id and RealFieldType_id =2 
    
    declare @PreparedPN nvarchar(200)='PreparedPN'
    declare @PreparedPN_id int= (select ID from dp_TablesColumns where  Table_ID =@table_id and ColumnName=@PreparedPN )
    
    if isnull(@PreparedPN_id,0) > 0 and  @pnFldId != @PreparedPN_id
     begin
    
       set @pnField= @PreparedPN
       set  @pnFldId=@PreparedPN_id
       
     end 
    
 declare @UnigramTerm_ID int= (select top(1) ID from rule_Terms where Name='Unigram' )

  declare @GLOSSARY_MAX_NGRAM_NUMBER int = dbo.fn_GetConfigValue('GLOSSARY_MAX_NGRAM_NUMBER')
  
 declare @maxawc int = isnull(( select MAX(a.WordsCount) 
											 	 from vw_AttrTerm a inner join
													     vw_AttrTerm at on at.Cat_ID =@comparableClass_id and at.ID=a.SubType_id 
												    where a.Cat_ID =@comparableClass_id and a.Type_id =5 and a.WordsCount<=@GLOSSARY_MAX_NGRAM_NUMBER),0)

 
  declare @res7 nvarchar(2000)=dbo.fn_GetMainResultTableName(@table_id,7,1)

create table #suppDel(trim_id int, flag int)
create table #suppDel3(trim_id int, flag int,flag2 int)
create table #suppDel32(trim_id int, flag int,flag2 int)
create table #suppDelStrFlag(trim_id int, value nvarchar(450),flag2 int)
create table #suppDelStrFlag2(trim_id int, value nvarchar(450),flag2 int)
create table #suppDelStr(value nvarchar(450),flag int)

  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#suppDelStr (value) include(flag)')	  
  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#suppDel (trim_id) include(flag)')	  

create table #cvt(ord int ,value nvarchar(450),wordType int null)
create table #cvt_prof(ord int ,value nvarchar(450),cvt_profile nvarchar(450))
create table #cvtLong(ord int ,value nvarchar(4000))
create table #prepRes(value nvarchar(4000), ord int primary key)

create table #zeropos(trim_id int, trim_rowid int, attr_id int,attr_rowid int)
create table #flowSearch(trim_id int, trim_rowid int,aw_id int, aw_rowid int ,wordscount int,zeroorder int,firstcharpos int)
create table #flowGrp(trim_id int, aw_id int,trim_startrow int, wordscount int,valuelen int,titleterm_id int,firstcharposition int)

create table #foundStopWords(trim_id int primary key)

create table #foundPhrasesForTitles(pn_id int, phrase nvarchar(450))

/* data for all results of search */

create table #productype_accounted_all_result(id int, expression nvarchar(450),pos int, posend int, fchar_pos int, expr_length int, expression_no_punct nvarchar(450), ord int identity  )
create table #productype_accountedrem_all_result(id int, expression nvarchar(450),pos int, posend int, fchar_pos int, expr_length int, expression_no_punct nvarchar(450), ord int identity  )


/* data for segment product type search*/
create table #sptPhrases(trim_id int, phrase nvarchar(450),phraseWC int, phraseLen int, phPosition_start int, phPosition_end int,ord int identity)

/* ========== BUFFER For SEARCH*/
create table #bbSrchRes(ord int ,pn_id int, attr_id int, titleterm_id int, confidenceLevel decimal(5,2) default 99.99, 
									   pos int, posend int,pnPrepwordform int, attrPrepForm int, srchtype int , cmptype int,
									  wordscount int, pnPrepwordlen int, firstcharposition int, pnrealwordlen int,
									   pnwordprep nvarchar(450), attrwordprep nvarchar(450), foundvalue nvarchar(450),ngrow_ord int)

create table #srchResult(trim_id int, posstart int, vallen int, attr_id int, Titleterm_id int, confidenceLevel decimal(5,2) default 99.99,
												 insideOfExpression smallint default 0, wordCount int, value nvarchar(450) default '', 
												 inConflict smallint default 1,beforestop nvarchar(450) default '',freq int default 0,maxfreq int default 0,
												 mapping nvarchar(450) default '', mapfreq int default 0, mapmaxfreq int default 0,
												foundAtStep int default -1, conflictsAtStep nvarchar(4000) default '' ,
												KPTConflicts nvarchar(max) default '', freq_account int default 0)

create table #grpBuff(ord int identity,pn_trim nvarchar(4000),pn_trimPrep nvarchar(4000))
create table #dblGrp(id int identity primary key, value nvarchar(450))


create table #stwrds(id int /*primary key*/,fw_id uniqueidentifier,
                                                  trim_Iteration int default 1,
								          pnLen  AS (cast(len(pn) as decimal(18,2))) PERSISTED, 
										pn_trimLen AS (cast(len(pn_trim) as decimal(18,2))) PERSISTED, 
										pn_trimBaseLen decimal(18,2),
										pn_trimTmpLen AS (cast(len(pn_trimTmp) as decimal(18,2))) PERSISTED, 
										pn_trimPreTrimLen AS (cast(len(pn_trimPreTrim) as decimal(18,2))) PERSISTED, 
										pn nvarchar(4000),
										pn_trimPreTrim nvarchar(4000) default '',
										pn_trimTmp nvarchar(4000) default '',
										pn_trimAfterStopWords nvarchar(4000) default '',
										pn_trimPreIterations nvarchar(4000) default '',
										pn_trim nvarchar(4000) default '',
										pn_trimPostIterations nvarchar(4000) default '',
										pn_remainder nvarchar(4000) default '',
										productype_accounted nvarchar(450) default '',
										productype_accounted_rem nvarchar(450) default '',
										productype_accounted_attribute nvarchar(450) default '',
										productype_reverse_accounted nvarchar(450) default '',
										productype_reverse_accounted_attribute nvarchar(450) default '',
										UnigramAccounting nvarchar(4000) default '',
										UnigramInProductTypeOrKilled nvarchar(4000) default '',
										UnigramUnaccounted  nvarchar(4000) default '',
										UnigramAccountingAfterPCH nvarchar(4000) default '',
										UnigramGroup nvarchar(4000) default '',
										ModifierAccounting nvarchar(4000) default '',
										ModifierInProductTypeOrKilled nvarchar(4000) default '',
										ModifiersUnaccounted  nvarchar(4000) default '',
										ModifierAccountingAfterPCH nvarchar(4000) default '',
										ComboSuggestion nvarchar(4000) default '',
										Last3Words nvarchar(450) default '',
										Last2Words nvarchar(450) default '',
										Last1Words nvarchar(450) default '',
										count1 int default 0,
										count2 int default 0, 
										count3 int default 0,
										productype_beforestop nvarchar(450) default '',
										producttype_noPlurals nvarchar(4000) default '',
										producttype_bookend nvarchar(450) default '',
										producttype_bookendWordsCount int default 0,
										BookendCompleteExpansions nvarchar(4000) default '',
										BookendDifferenceExpansions nvarchar(4000) default '',
										producttype_unigram nvarchar(450) default '',
										FinalProductType nvarchar(450) default '',
										FinalProductTypeFreq int default 0,
										SegmentProductType nvarchar(450) default '',
										Count_segPT int default 0,
										Length_segPT int default 0,
										SegmentProductType_id int default 0,
										SegmentNumber decimal(5,1) default 0,
										SegmentProductType_Confidence nvarchar(100) default 'high',
										SegmentProductType_Quality nvarchar(100) default 'high', 
										SegmentProductType_no_punct nvarchar(450) default '',
										SegmentProductType_In_WrongPlace nvarchar(10) default 'No',
										GoogleTaxonomy nvarchar(max) default '',
										GoogleTaxonomy_Rules nvarchar(max) default '',
										PT_GoogleTaxonomy nvarchar(max) default '',
										SegmentProductType2Dan nvarchar(450) default '',
										SegmentProductType2TypeDan nvarchar(150) default '',
										SegmentProductType2Category nvarchar(450) default '',
										SegmentProductType2TypeCategory nvarchar(450) default '',
										SegmentProductType2CategoryAcrossID int default 0,
										productype_accounted_all nvarchar(max) default '',
										productype_accounted_rem_all nvarchar(max) default '',
										GoogleTaxonomy_accounted_all nvarchar(max) default '',
										GoogleTaxonomy_remainders_all nvarchar(max) default '',
										Last_Node_of_MC nvarchar(450) default '',
										Last_Node_Num_of_PT_found int default 0,
										Last_Node_PT_Found nvarchar(4000) default '',
										Last_Node_PT_Remainder nvarchar(4000) default '',
										Last_Node_PT_Quality nvarchar(4000) default '',
										Last_Node_PT_Skip_Type nvarchar(50) default '',
										Last_Node_PT_Skipped nvarchar(4000) default '',
										KPTConflicts nvarchar(max) default '', 
										KPTFrequency nvarchar(1000) default '', 
										KPTMaxFrequency nvarchar(1000) default '',
										KPTMappingFrequency nvarchar(1000) default '', 
										KPTMappingMaxFrequency nvarchar(1000) default '',
										KPTFountAtStep int default -1,
										KPTConflictSteps nvarchar(4000) default '',
										dg_PNTrimRestoredStage int default 0,
										dg_PhrasePos int default -1,
										dg_PhraseFound nvarchar(450) default '',
										dg_StopWordPos int default -1,
										dg_StopWords nvarchar(4000) default '',
										dg_HphAndPunctPos int default -1,
										dg_OtherPunctPos int default -1,
										dg_BeforeStopPos int default -1,
										dg_TrimStateBeforeFinalCheck  nvarchar(max) default ''
										)
  
declare @PTColumn nvarchar(128)

if @getbyProductType=1
     set @PTColumn= (select top(1) ColumnName  from dp_TablesColumns where Table_ID=@table_id and RealFieldType_id = 202 ) /*Product type*/

declare @expr nvarchar(max)
declare @haveTestPN bit = case when isnull(@testPN,'')='' then 0 else 1 end

if @haveTestPN=0
 begin
 
	    set @expr =' insert into #stwrds(id,fw_id,pn,pn_trim,pn_trimBaseLen)
				 select   r.force_id_identity,r.fw_id,trval.val,trval.val, len(trval.val) 
					    from '+ @res7+' r  cross apply
						   (select ltrim(rtrim(r.['+@pnField+'])) val ) trval 
					where  trval.val != '''' '+ 
									    case when @PTColumn is not null then ' and r.['+@PTColumn+'] = '''' ' else '' end+	  
									    case when @fixed_id != '' then ' and r.force_id_identity in ('+convert(nvarchar,@fixed_id) +')'
				 else '' end 

		exec(@expr)
		
end		
else
begin

	   set @expr=' insert into #stwrds(id,fw_id,pn,pn_trim,pn_trimBaseLen)
				  select   1 as force_id_identity, NEWID() fw_id, trval.val, trval.val, len(trval.val) 
					from (select '''+replace(@testPN,'''','''''')+''' val ) trval '

	   exec(@expr)
	   
	   set @test_ids='1'
	   
end

 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_id ON dbo.#stwrds (id)')

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

/* ==  temporary test replace*/

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Remove wrappers for Color field type'

    declare @colorwrapper_begin nvarchar(450) ,@colorwrapper_end nvarchar(450)

    select @colorwrapper_begin= TextWrapperBegin,@colorwrapper_end=TextWrapperEnd  from fp_FieldTypes  where TextWrapperBegin !='' and id =205

    if @colorwrapper_begin is not null
      begin
	    create table #ocol(id int primary key)

	    update #stwrds set pn=ltrim(rtrim(dbo.RemoveDuplicates(replace(replace(pn, @colorwrapper_begin,''),@colorwrapper_end,''),' ')))
	    output inserted.id into #ocol(id)

	    update #stwrds set pn_trim= pn , pn_trimBaseLen=LEN(pn)
       from #stwrds s  inner join
                #ocol r on r.id =s.id 


	 end

create table #rplone(value nvarchar(4000),ord int)

   insert into #rplone(value,ord)
    exec RegexpReplaceBatch @fieldname='pn', @identityName='id',
														    @dataSource='select id,pn from #stwrds', @dataSourceAsTable=0,
                                                            @patternTable ='select ''(?:''+dbo.fn_RegexEscape(TextWrapperBegin)+'')|(?:'' +dbo.fn_RegexEscape(TextWrapperEnd)+'')'',''''
																		  from fp_FieldTypes  where TextWrapperBegin !='''' and id =205
                                                                                                union all
																				    select ''\s{2,}'', '' '' ' ,
															 @patternTableAsTable=0, @CaseSensitive=0,
															 @singlepattern='', @singlereplacewith=0,@returnAllResult=0

 update #stwrds set pn=  ltrim(rtrim(r.value)),pn_trim=  ltrim(rtrim(r.value)),pn_trimBaseLen=LEN( ltrim(rtrim(r.value)))
       from #stwrds s  inner join
                #rplone r on r.ord =s.id 

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID


if @test_ids != ''
 begin
		   select cast('Enter' as nvarchar(450)) as operation, CAST('Original data' as nvarchar(450)) as Iteraton, *  into #stwrds_test_result 
					from #stwrds 
					where id in (select t.IntNumber from dbo.IntStringToTable( @test_ids,',',0) t) 
					order by id
					
			select cast('Enter' as nvarchar(450)) as operation, CAST('Original data' as nvarchar(450)) as Iteraton,* 	into #srchResult_test from #srchResult	

		    alter table #srchResult_test add ord int identity 

			select cast('Enter' as nvarchar(450)) as operation, CAST('Original data' as nvarchar(450)) as Iteraton,* , CAST('' as nvarchar(450)) Value,
					  CAST('' as nvarchar(450)) Attribute into #bbSrchRes_test from #bbSrchRes	
		
end

/* stop words which generate PT*/
create table #StopWordsGenPT(id int, minpos int default -1, maxpos int, foundname nvarchar(450), foundFirstPos int,exceptionLastPos int default 0, GenPT int default 0)

  exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Trimming PNs'

 exec DC_SaaSProductTypediscovery_trimPN  @comparableClass_id=@comparableClass_id,@test_ids=@test_ids,@table_id=@table_id, @test_id=@test_id
 
 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#StopWordsGenPT (id) include(foundFirstPos,exceptionLastPos,GenPT)')	  
  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#stwrds (id) include(pn_trim,pn_remainder)')	  

--#region All tokens of trimmed PNs

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Collect ALL tokens from trimmed PNs'

create table #TrimmedTokensAll(ord int identity  ,trim_id int, pos int, posend int, wordcount int, Uniq_id int default 0, endpos int, endposend int,firstcharpos int,  token nvarchar(450))

insert into #TrimmedTokensAll(trim_id, token, wordcount,pos,posend,endpos,endposend,firstcharpos)
select d.id  , z.name , z.numwords ,z.pos,z.posend,z.endpos,z.endposend,z.firstcharpos
	 from #stwrds d cross apply
			dbo.GetNGramsFromStringStd(d.pn_trim,1,@maxawc,0,0)  z 

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

     exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Collect ALL tokens from trimmed PNs : Indexing and sync IDs'

  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_1 ON dbo.#TrimmedTokensAll (wordcount) include(trim_id,pos,token,firstcharpos)')	  
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_2 ON dbo.#TrimmedTokensAll (trim_id,pos) include(ord)')	  
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_3 ON dbo.#TrimmedTokensAll (token) include(wordcount,trim_id,firstcharpos)')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_4 ON dbo.#TrimmedTokensAll (wordcount, endposend)  include(trim_id, token)')

create table #tokensAllUniq( id int identity, freq int, wordcount int,tokenlen int, token nvarchar(450))

insert into #tokensAllUniq( token, wordcount,freq,tokenlen)
 select z.token ,max(z.wordcount) wc ,count(distinct z.trim_id) as freq ,LEN(z.token) as tokenlen
   from #TrimmedTokensAll  z
 group by z.token

 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #tokensAllUniq (token) include(id,tokenlen,freq,wordcount)')
 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #tokensAllUniq (id) include(token,tokenlen,freq,wordcount)')
 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #tokensAllUniq (wordcount,freq) include(token)')

update #TrimmedTokensAll  set Uniq_id=t.id 
           from #TrimmedTokensAll ta inner join
				#tokensAllUniq t on t.token =ta.token    

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID
				
--#endregion

--#region Prepared Data For Search

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare EXACT data with length >1 from trimmed PNs'


/*exact data (no changes) , data with single word in a position*/
create table #pnTrimWordsExact(id int,rowID int,firstcharpos int,wordscount int, Part nvarchar(450))

insert into #pnTrimWordsExact(id,rowID, firstcharpos, Part,wordscount )
select k.trim_id,k.pos,k.firstcharpos+1,k.token ,k.wordscount
from (
			 select ta.trim_id,ta.pos+1 pos ,ta.firstcharpos ,ta.token,(MAX(ta.pos+1) over (partition by ta.trim_id)) wordscount
		  	    from #TrimmedTokensAll ta 
			 where ta.wordcount=1   
		  ) k  where k.wordscount >1
	  
 set  @uidxname = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #pnTrimWordsExact (id) include(rowID,wordscount,part,firstcharpos)')
 set  @uidxname = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #pnTrimWordsExact (id,rowID) include(wordscount,part,firstcharpos)')

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID


/*data for account*/

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare data of trimmed PNs for search'

declare @preNgramXmlReplace nvarchar(4000)=
(select *
    from ( values ('-' ,char(2),'BETWEENNUMBERS'),
					('/' ,char(3),'BETWEENNUMBERS'),
	   			 	   ( '/',' ','SIMPLEREPLACE' ),
		  	  		   ( '-',' ','SIMPLEREPLACE')
) d(tmpl,repl,type_e) for xml path('node'), root('root'))


declare @postNgramXmlReplace nvarchar(4000)=
(select * from ( values (CHAR(2) ,'-','BETWEENNUMBERS' ) 
										  ,  (CHAR(3) ,'/','BETWEENNUMBERS' ) 
) u(tmpl,repl,type_e) for xml path('node'), root('root'))

declare @cleanNgramXmlReplace nvarchar(4000)=
(select *
	  from ( values  ('(?:(?<=^|\s)[^0-9a-zA-Z\s]+?(?=[a-zA-Z]))|(?:(?<=[a-zA-Z0-9])[^0-9a-zA-Z\s]+?(?=$|\s))|(?:(?<=^|\s)[^0-9a-zA-Z\s]{2,}?(?=$|\s))','','REGEX'),
						 (' ','','DUPLICATES') 
						 ) d(k,i,type_e) for xml path('node'), root('root'))

declare @TrimmedNgramsTBName nvarchar(128)= '##TrimmedNgrams'+replace(convert(varchar(64),newid()),'-','_')
exec('create table  '+@TrimmedNgramsTBName+'(id int, grp_ord int)')
declare @TrimmedNgramsDataTBName nvarchar(128)= '##TrimmedNgramsData'+replace(convert(varchar(64),newid()),'-','_')
exec('create table '+@TrimmedNgramsDataTBName+'(ord int, numwords int, pos int, posend int, endpos int, endposend int, firstcharpos int,realnameLen int, preparednameLen int, wordscountPrepared int,
									  valueform int default -2, name nvarchar(450),namePrepared nvarchar(450),namesingular nvarchar(450), row_ord int )')

exec [PrepareSearchData]
@srcQuery ='select id , pn_trim from #stwrds',
@indetityName ='id',
@valueName ='pn_trim',
@preNgramXmlReplace =@preNgramXmlReplace,
@generateNgrams  =1,
@minGnWC  =1, 
@maxNGWC =@maxawc,
@postNgramXmlReplace =@postNgramXmlReplace,
@cleanNgramXmlReplace=@cleanNgramXmlReplace,
@englishUseInternalRules =1,
@englishRulesQuery ='select * from PluralSingularRules',
@allRowsDataTable =@TrimmedNgramsTBName,
@groupedDataTable =@TrimmedNgramsDataTBName,
@writeBatchSize=10000


exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare data of Attributes for search'

declare @preNgramXmlReplaceAttr nvarchar(4000)=
(select *
    from ( values ( '/',' ','SIMPLEREPLACE' ),
		  	  		      ( '-',' ','SIMPLEREPLACE')

) d(tmpl,repl,type_e) for xml path('node'), root('root'))


create table #attribs(id int,wordscount int,
							  pos int, posend int, endpos int, endposend int, firstcharpos int,
							  namelen int, preparedlength int,preparedwordcount int,valueform int default -2,
							  name nvarchar(450), nameprepared nvarchar(450), namesingular nvarchar(450), row_ord int,
							  titleTerm_id int, title_type int , titlename nvarchar(450),
							  nameprepReverse nvarchar(450),nameSingReverse nvarchar(450) )

create table #attr_bf_tmp(id int, name nvarchar(450),title_type int,titleTerm_id int,titlename nvarchar(450))


declare @tmp_perf int

    exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare data of Attributes for search: Get attributes'

insert into #attr_bf_tmp(id , name,title_type,titleTerm_id,titlename)
select a.ID,a.Name,
		   case when left(at.Name,3) = 'fw-' then 1
					    when  at.Name = 'Unigram' then 2
					    when at.Name = 'Modifier' then 3
					    else 0 end, at.Term_id,at.Name
     from vw_AttrTerm a inner join
              vw_AttrTerm at on at.Cat_ID =@comparableClass_id and at.ID=a.SubType_id and
																				((( left(at.Name,3) = 'fw-' or at.Name = 'Unigram' or at.Name = 'Modifier')  and a.WordsCount <= @maxawc) or
																				   a.WordsCount between 2 and @maxawc) 
 where a.Cat_ID =@comparableClass_id and a.Type_id =5

 exec dp_Tests_Performance_SetStop @id=@tmp_perf

     exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare data of Attributes for search: Batch preparation'

  exec [PrepareSearchData]
@srcQuery ='select id,name from #attr_bf_tmp',
@indetityName ='id',
@valueName ='name',
@preNgramXmlReplace =@preNgramXmlReplaceAttr,
@generateNgrams  =0,
@minGnWC  =1, 
@maxNGWC =@maxawc,
@postNgramXmlReplace ='',
@cleanNgramXmlReplace=@cleanNgramXmlReplace,
@englishUseInternalRules =1,
@englishRulesQuery ='select * from PluralSingularRules',
@allRowsDataTable ='#attribs',
@groupedDataTable ='',
@writeBatchSize=10000

 exec dp_Tests_Performance_SetStop @id=@tmp_perf

      exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare data of Attributes for search: Final stage'

 update #attribs set nameprepReverse= dbo.reversestring(a.nameprepared ,0), nameSingReverse=dbo.reversestring(a.namesingular ,0)
      from #attribs a
      
declare @uidxname_at_tmpi varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_at_tmpi+' ON dbo.#attr_bf_tmp(id) include(title_type,titleTerm_id,titlename)')

update #attribs set title_type=t.title_type,titleTerm_id=t.titleTerm_id,titlename=t.titlename
  from #attribs a inner join
           #attr_bf_tmp t on t.id=a.id

 exec dp_Tests_Performance_SetStop @id=@tmp_perf

exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Indexing search data'

   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_2t ON '+@TrimmedNgramsDataTBName+
								' (numwords,valueform) include(ord,pos,posend,firstcharpos,realnameLen,preparednameLen,namePrepared,namesingular,row_ord)')	  
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_3t ON '+@TrimmedNgramsDataTBName+
								' ([endposend], [numwords]) INCLUDE ([ord], [pos], [posend], [firstcharpos], [realnameLen], [preparednameLen], [namesingular], [row_ord])')	  
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_5t ON '+@TrimmedNgramsDataTBName+
								' ([numwords], [endposend]) INCLUDE ([ord], [pos], [posend], [firstcharpos], [realnameLen], [preparednameLen], [namesingular], [row_ord])')	  
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_6 ON '+@TrimmedNgramsDataTBName+
								' (name) INCLUDE (ord)')	  
								
								

   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
    exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_1a on #attribs  (title_type)  INCLUDE (id,name,valueform)')

   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
    exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_2a on #attribs  (title_type,id)  INCLUDE (name,valueform)')
    
   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
   exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_3a on #attribs (preparedwordcount,title_type) 	 INCLUDE (id,titleTerm_id,nameprepared,valueform)')

   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
   exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_4a on #attribs (preparedwordcount,title_type,valueform) INCLUDE (id,titleTerm_id,nameprepared)')

   set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
   exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+'_5a on #attribs (id) INCLUDE (namelen,[name],[titlename])')

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

--#region FOUND AT THE END OF TRIMMED

 exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Find at the end of trimmed PNs'

truncate table #bbSrchRes

exec DC_SaaSProductTypediscovery_RegularSearch  @maxawc =@maxawc,@UnigramTerm_ID=@UnigramTerm_ID, @rightanchor=1,
																							   @skipInnerFilter=0,@freeFilter = 'exists(select 1 from #foundStopWords stw where stw.trim_id=tn.id)',
																							   @searchTarget=1,@test_searchtarget='Before Stop'   ,@test_ids=@test_ids,
																							   @PNTableName=@TrimmedNgramsTBName,@PNDataTableName=@TrimmedNgramsDataTBName

update #stwrds  set productype_beforestop = SUBSTRING(s.pn_trim,p.firstcharposition+1, p.pnrealwordlen), dg_BeforeStopPos=p.firstcharposition+1
      from #stwrds s inner join
                #bbSrchRes p on p.pn_id =s.id 

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

--#region ACCOUNT

declare @writeTestData  bit =0

if isnull(@test_ids,'') != ''
begin
 exec('insert into #stwrds_test_result
		  select ''Account'' as Operation,''Start State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')
		  set @writeTestData = 1
end

exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Accounting'

exec DC_SaaSProductTypediscovery_Account @maxawc=@maxawc,@UnigramTerm_ID=@UnigramTerm_ID,@test_ids=@test_ids,
																			@TrimmedNgramsTBName=@TrimmedNgramsTBName,@TrimmedNgramsDataTBName=@TrimmedNgramsDataTBName,
																			@writeTestData=@writeTestData,@table_id=@table_id, @test_id=@test_id	

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

if isnull(@test_ids,'') != ''
 exec('insert into #stwrds_test_result
		  select ''Account'' as Operation,''Finish State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

--#endregion

if  isnull(@test_ids,'') != '' and @haveTestPN=0 and (@exec_level = '' or @exec_level='acc')
begin
		  
  exec('select * from #productype_accounted_all_result where id in ('+@test_ids+') order by id')
  exec('select * from #productype_accountedrem_all_result where id in ('+@test_ids+') order by id')
  
  exec('select * from #stwrds where id in ('+@test_ids+') order by id')
  
  exec('select * from #stwrds_test_result where id in ('+@test_ids+') order by id')
  exec('select * from #bbSrchRes_test where pn_id in ('+@test_ids+') order by pn_id')
  exec('select * from #srchResult_test where trim_id in ('+@test_ids+') order by ord')
   
   
   if (@exec_level = '' or @exec_level='acc')
    return
end

--#region Account against Remainders

 rem_test:

exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare remainders for search'


declare @TrimmedNgramsTBNameRem nvarchar(128)= '##TrimmedNgramsRem'+replace(convert(varchar(64),newid()),'-','_')
exec('create table  '+@TrimmedNgramsTBNameRem+'(id int, grp_ord int)')
declare @TrimmedNgramsDataTBNameRem nvarchar(128)= '##TrimmedNgramsDataRem'+replace(convert(varchar(64),newid()),'-','_')
exec('create table '+@TrimmedNgramsDataTBNameRem+'(ord int, numwords int, pos int, posend int, endpos int, endposend int, firstcharpos int,realnameLen int, preparednameLen int, wordscountPrepared int,
									  valueform int default -2, name nvarchar(450),namePrepared nvarchar(450),namesingular nvarchar(450), row_ord int )')

exec [PrepareSearchData]
@srcQuery ='select id , pn_remainder from #stwrds s where s.pn_remainder !='''' ',
@indetityName ='id',
@valueName ='pn_remainder',
@preNgramXmlReplace =@preNgramXmlReplace,
@generateNgrams  =1,
@minGnWC  =1, 
@maxNGWC =@maxawc,
@postNgramXmlReplace =@postNgramXmlReplace,
@cleanNgramXmlReplace=@cleanNgramXmlReplace,
@englishUseInternalRules =1,
@englishRulesQuery ='select * from PluralSingularRules',
@allRowsDataTable =@TrimmedNgramsTBNameRem,
@groupedDataTable =@TrimmedNgramsDataTBNameRem,
@writeBatchSize=10000

declare @uidxname_rem_rem varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_rem_rem+'_1 ON dbo.'+@TrimmedNgramsDataTBNameRem+' ([numwords]) INCLUDE ([ord], [pos], [posend], [firstcharpos], [realnameLen], [preparednameLen], [namesingular], [row_ord])')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_rem_rem+'_2 ON dbo.'+@TrimmedNgramsDataTBNameRem+' ([numwords], [valueform]) INCLUDE ([ord], [pos], [posend], [firstcharpos], [realnameLen], [preparednameLen], [namesingular], [row_ord])')

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

   exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Account against Remainders'

				    truncate table #bbSrchRes

				    declare @tmp_test_prm nvarchar(4000)=''

				    if @fixed_id != '' 
				            set @tmp_test_prm = @fixed_id
				    else if @test_ids != '' 
				            set @tmp_test_prm = @test_ids


				    exec DC_SaaSProductTypediscovery_RegularSearch  @maxawc =@maxawc,@UnigramTerm_ID=@UnigramTerm_ID, @rightanchor=0,@skipInnerFilter=0,
																								 @PNTableName=@TrimmedNgramsTBNameRem,@PNDataTableName=@TrimmedNgramsDataTBNameRem,
																								@test_ids=@tmp_test_prm ,@searchTarget=0,@test_searchtarget='Remainders' 


				 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
				 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#bbSrchRes (pn_id) include(firstcharposition,pnrealwordlen)')

				    update #stwrds  set productype_accounted_rem =SUBSTRING(s.pn_remainder,p.firstcharposition+1,p.pnrealwordlen)
					from #stwrds s inner join
							#bbSrchRes p on p.pn_id =s.id

				    update #stwrds  set productype_accounted_attribute =a.titlename 
					from #stwrds s inner join
								#bbSrchRes p on p.pn_id =s.id inner join
								#attribs a on a.id =p.attr_id   

				    exec('drop INDEX idx_#tr_'+@uidxname+' ON dbo.#bbSrchRes')

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Remainders'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')


            if @exec_level='rem_acc'
			 begin
			 
			     if  @test_ids != ''
			         exec(' select ''remainder_account'' stage ,  * from #stwrds where id in ('+@test_ids+') order by id') 

			     if  @fixed_id != '' and   @fixed_id != @test_ids
			         exec(' select ''remainder_account'' stage ,  * from #stwrds where id in ('+@fixed_id+') order by id') 

				 exec('select * from #stwrds_test_result where id in ('+@test_ids+') order by id')
				 exec('select * from #bbSrchRes_test where pn_id in ('+@test_ids+') order by pn_id')
				 exec('select * from #srchResult_test where trim_id in ('+@test_ids+') order by trim_id')

				 exec('select * from #productype_accountedrem_all_result where id in ('+@test_ids+') order by id')
			          
				 return
				    			 
			 end
/*end*/

--#endregion

 --#region Last 3 words

     exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Last 1,2,3 words'

update #stwrds set Last1Words=t.token
    from #stwrds s inner join
             #TrimmedTokensAll t on t.trim_id=s.id and t.endposend=0 and t.wordcount=1

update #stwrds set Last2Words=t.token
    from #stwrds s inner join
             #TrimmedTokensAll t on t.trim_id=s.id and t.endposend=0 and t.wordcount=2

update #stwrds set Last3Words=t.token
    from #stwrds s inner join
             #TrimmedTokensAll t on t.trim_id=s.id and t.endposend=0 and t.wordcount=3
 
 set @uidxname = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#stwrds (id) include(Last3Words)')
 set @uidxname1 = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname1+' ON dbo.#stwrds (id) include(Last2Words)')
 set @uidxname2 = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname2+' ON dbo.#stwrds (id) include(Last1Words)')

insert into #suppDelStr(value, flag)
select Last1Words,count(id)  from  #stwrds group by Last1Words 

update #stwrds  set count1 =cte.flag 
        from  #suppDelStr cte inner join
                 #stwrds s on cte.value =s.Last1Words  

truncate table #suppDelStr
insert into #suppDelStr(value, flag)
select Last2Words,count(id)  from  #stwrds group by Last2Words 

update #stwrds  set count2 =cte.flag 
        from #suppDelStr cte inner join
                 #stwrds s  on cte.value =s.Last2Words  

truncate table #suppDelStr
insert into #suppDelStr(value, flag)
select Last3Words,count(id)  from  #stwrds group by Last3Words 

update #stwrds  set count3 =cte.flag 
        from  #suppDelStr cte inner join
                  #stwrds s on cte.value =s.Last3Words  

 exec('Drop INDEX idx_#tr_'+@uidxname+' ON dbo.#stwrds')
 exec('Drop INDEX idx_#tr_'+@uidxname1+' ON dbo.#stwrds')
 exec('Drop INDEX idx_#tr_'+@uidxname2+' ON dbo.#stwrds')

    exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

  
--#region REVERSE  ACCOUNT

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Reverse account'

truncate table #bbSrchRes


exec DC_SaaSProductTypediscovery_RegularSearch  @maxawc =@maxawc,@UnigramTerm_ID=@UnigramTerm_ID, @rightanchor=0,@skipInnerFilter=0,
																			  @attributeNameField='nameSingReverse',@attributePreparedFieldName='nameprepReverse',@searchTarget=1
																			 ,@test_searchtarget='Reverse Account'   ,@test_ids=@test_ids,
																			  @PNTableName=@TrimmedNgramsTBName,@PNDataTableName=@TrimmedNgramsDataTBName 


  set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#bbSrchRes (pn_id) include(firstcharposition,pnrealwordlen)')

update #stwrds  set productype_reverse_accounted =SUBSTRING(s.pn_trim,p.firstcharposition+1,p.pnrealwordlen)
      from #stwrds s inner join
                #bbSrchRes p on p.pn_id =s.id

update #stwrds  set productype_reverse_accounted_attribute =a.titlename 
      from #stwrds s inner join
                #bbSrchRes p on p.pn_id =s.id inner join
                #attribs a on a.id =p.attr_id   

exec('drop INDEX idx_#tr_'+@uidxname+' ON dbo.#bbSrchRes')

    exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

if isnull(@test_ids,'') != ''
 exec('insert into #stwrds_test_result
		  select ''Reverse Account'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

--#endregion


--#region  ------ Bookend ----

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookend prepare attributes'

create table #AttribsWordsExact(id int,rowID int, Part nvarchar(450),wordscount int)

   insert into #AttribsWordsExact(id,rowID, Part,wordscount)
	select d.id,k.rowID,k.Part ,d.WordsCount 
				 from #attribs  d  cross apply
					     dbo.SplitStringToTable(d.name, ' ',0) k  
	where d.wordscount>1 and d.title_type between 1 and 2	
	
 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #AttribsWordsExact (id) include(rowID,wordscount,part)')
 set @uidxname  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo. #AttribsWordsExact (id,rowID) include(wordscount,part)')

     exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch'

    declare @p_cke int

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 1'

truncate table #flowSearch 

insert into #flowSearch(trim_id, trim_rowid,aw_id, aw_rowid,wordscount,zeroorder, firstcharpos  ) 
select  tw2.id, tw2.rowID,aw2.id ,aw2.rowID ,aw2.wordscount,tw.rowID,tw.firstcharpos
    from #pnTrimWordsExact tw inner join
	        #AttribsWordsExact aw on  aw.rowID =0 and tw.wordscount > aw.wordscount and (tw.wordscount -tw.rowID +1) >= aw.wordscount  and  tw.Part =aw.Part inner join
		   #pnTrimWordsExact tw2 on tw2.id=tw.id  and tw.rowid< tw2.rowID inner join
		   #AttribsWordsExact aw2 on  aw.id =aw2.id and aw.rowid < aw2.rowID and  tw2.Part =aw2.Part

     exec dp_Tests_Performance_SetStop @id=@p_cke
 			
    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 2'

create table #grpZero(trim_id int, attr_id int, zero_row_id int,firstcharpos int) 

insert into  #grpZero(trim_id , attr_id , zero_row_id,firstcharpos ) 
select b.trim_id,b.aw_id,b.zeroorder,b.firstcharpos
  from #flowSearch b
group by b.trim_id,b.aw_id,b.zeroorder,b.firstcharpos 
 
     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 3'

insert into #flowSearch(trim_id, trim_rowid,aw_id, aw_rowid,wordscount,zeroorder,firstcharpos  ) 
select  b.trim_id,b.zero_row_id,b.attr_id,0,aw.wordscount,b.zero_row_id,b.firstcharpos
   from #grpZero b inner join
		  #pnTrimWordsExact tw  on tw.id=b.trim_id and tw.rowID=b.zero_row_id inner join
	        #AttribsWordsExact aw  on aw.id=b.attr_id and aw.rowID=0

     exec dp_Tests_Performance_SetStop @id=@p_cke

truncate table #flowGrp

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 4'

    create table #flowGrp_pre(trim_id int, aw_id int,wordscount int ,firstcharposition int)

	   insert into #flowGrp_pre(trim_id, aw_id,wordscount,firstcharposition )  -- collect found attributes 
	   select z.trim_id, z.aw_id,z.wordscount,z.firstcharpos
	   from (
		   select  fl.trim_id, fl.aw_id,fl.wordscount ,COUNT(distinct fl.aw_rowid) attrcnt ,COUNT(distinct fl.trim_rowid) trimcnt,fl.firstcharpos
				 from #flowSearch fl 
		   group by  fl.trim_id, fl.aw_id,fl.wordscount,fl.zeroorder,fl.firstcharpos
	   ) z 
	   where z.wordscount = z.attrcnt and z.attrcnt =z.trimcnt

	   insert into #flowGrp(trim_id, aw_id,wordscount,firstcharposition,valuelen )  -- collect found attributes 
	   select z.trim_id, z.aw_id,z.wordscount,z.firstcharposition,a.namelen
	   from #flowGrp_pre z inner join
			  #attribs a on a.id=z.aw_id


     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 5'

delete from #flowGrp        --- delete which are part of Phrases of trimmed PN
      from #flowGrp z inner join
               #sptPhrases p on p.trim_id=z.trim_id and p.phraselen>z.valuelen and
												z.firstcharposition between p.phPosition_start and p.phPosition_end 

delete from #flowGrp        --- delete which equals to accounted PT
      from #flowGrp p inner join
                #stwrds s on p.trim_id =s.id inner join
                #attribs  t on t.id =p.aw_id  and t.name =s.productype_accounted   

create table #beanchors(trim_id int,aw_id int)

     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 6'

insert into #beanchors(trim_id,aw_id)   -- get pairs with max length for PT
select k.trim_id, MAX(pt.aw_id)  
   from ( 
				 select t.trim_id,max(t.wordscount) wordscount  
						from #flowGrp t 
				   group by t.trim_id
			  ) k inner join
			     #flowGrp pt on pt.trim_id =k.trim_id and pt.wordscount =k.wordscount   
 group by k.trim_id

 declare @flsuidxname varchar(64)  = replace(convert(varchar(64),newid()),'-','_')
 exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@flsuidxname+' ON dbo.#flowSearch (trim_id,aw_id,trim_rowid)')
 
create table #bewords(trim_id int, aw_id int,minrow_id int, maxrow_id int,expression nvarchar(4000) default '', bename nvarchar(4000),diffexpr nvarchar(4000))

insert into  #bewords(trim_id, aw_id ,minrow_id, maxrow_id)  -- get PT and attribute and Begin End positions
select  fs.trim_id,fs.aw_id ,MIN(fs.trim_rowid), MAX(fs.trim_rowid)   
      from #flowSearch fs inner join
                #beanchors ba on ba.trim_id =fs.trim_id and ba.aw_id =fs.aw_id   
  group by fs.trim_id,fs.aw_id 

delete from #bewords where minrow_id =maxrow_id  -- BE positions is the same must be ignored

     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 6.1'

update #bewords set expression =e.expr  -- get expression between BE
        from #bewords w cross apply
                  (select  dbo.StrConcatWithDlm(tw.Part ,' ',0) expr 
                           from  #pnTrimWordsExact tw 
                           where tw.id =  w.trim_id and tw.rowID between w.minrow_id and w.maxrow_id) e  

update #bewords  set bename  =aw.Name  -- get attribute
      from #bewords s inner join
                #beanchors p on p.trim_id =s.trim_id  inner join
                #attribs aw on aw.ID =p.aw_id   

delete from #bewords where bename =expression  -- ignore when BE is the attribute name

     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 6.2'

update #bewords set diffexpr =e.expr   -- difference in attribute and PN trim  SLOW PART
        from #bewords w cross apply
                  (select  dbo.StrConcatWithDlm(tw.Part ,' ',0) expr 
                           from  #pnTrimWordsExact tw 
								 where tw.id =  w.trim_id and tw.rowID between w.minrow_id and w.maxrow_id and
								               not exists(select 1 from #flowSearch fs where fs.trim_id =w.trim_id  and fs.aw_id =w.aw_id and fs.trim_rowid =tw.rowID )) e  

delete from #bewords where diffexpr =''  -- ignore if attribute and content of PN is the same

delete from #beanchors 
       from #beanchors a
       where not exists(select 1 from #bewords w where w.trim_id =a.trim_id) 

     exec dp_Tests_Performance_SetStop @id=@p_cke

    exec @p_cke= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Bookendsearch: 6.3'

update #stwrds  set producttype_bookend  =aw.Name ,producttype_bookendWordsCount= aw.wordscount 
      from #stwrds s inner join
                #beanchors p on p.trim_id =s.id inner join
                #attribs   aw on aw.ID =p.aw_id   
  
update #stwrds  set BookendCompleteExpansions  =p.expression,BookendDifferenceExpansions =p.diffexpr 
      from #stwrds s inner join
                #bewords p on p.trim_id =s.id 

 exec('drop INDEX idx_#tr_'+@flsuidxname+' ON dbo. #flowSearch')

      exec dp_Tests_Performance_SetStop @id=@p_cke

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Bookend'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')


--#endregion

--#region ==== Unigrams Metadata

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Unigrams metadata'

truncate table #bbSrchRes

exec DC_SaaSProductTypediscovery_RegularSearch  @maxawc =1,@UnigramTerm_ID=0, @rightanchor=0,@skipInnerFilter=1,
																			  @attributeNameField='nameprepared'	,@PNNameField='nameprepared',
																			 @attributeFilter= 'a.title_type between 2 and 3',@searchTarget=1
																			 ,@test_searchtarget='Unigrams Metadata'   ,@test_ids=@test_ids,
																			 @PNTableName=@TrimmedNgramsTBName,@PNDataTableName=@TrimmedNgramsDataTBName
 
create table #unifound(id int, uniid int,ptfound bit default 0,ord int identity)

insert into #unifound(id, uniid)
select r.pn_id,r.attr_id
   from #bbSrchRes r
group by r.pn_id,r.attr_id
 
-- for set/kit plurals
 
 create table #UA2Words(id int,name nvarchar(450))
  insert into #UA2Words(id ,name)
   select s.id, j.name
        from #stwrds s cross apply
                (select a.name
                       from  #unifound u  inner join
						   #attribs a on a.ID=u.uniid    
					   where  s.id=u.id	and a.title_type =2		 
				) j	   
     group by  s.id,j.name
 
 declare @uidxnameUA varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxnameUA+' ON dbo.#UA2Words (id) include(name)')
 
create table #ugtmp(id int, name nvarchar(4000)) 

insert into #ugtmp(id ,name) 
select u.id,dbo.StrConcatWithDlm( a.name,' || ' ,1) 
                       from  #unifound u  inner join
						   #attribs a on a.ID=u.uniid    
					   where   a.title_type =2
group by  u.id					   

declare @uidxname_ugt varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_ugt+' ON dbo.#ugtmp (id) include(name)')


-- fill unigrams

update #stwrds set UnigramAccounting =t.name
        from #stwrds s inner join
                 #ugtmp t on s.id=t.id 

truncate table #ugtmp

insert into #ugtmp(id ,name) 
select u.id,dbo.StrConcatWithDlm( a.name,' || ' ,1) 
                       from  #unifound u  inner join
						   #attribs a on a.ID=u.uniid    
					   where   a.title_type =3
group by  u.id					   


-- fill modifiers
update #stwrds set ModifierAccounting =t.name   
        from #stwrds s inner join
                 #ugtmp t on s.id=t.id 


-- set found in KPT
update #unifound set ptfound =1
     from #unifound uf inner join
               #stwrds s on s.id=uf.id cross apply
               (select N' '+replace(s.productype_accounted,'/',' ') +N' ' productype_accounted ) z inner join
                #attribs a on  a.id =uf.uniid and  CHARINDEX(' '+a.name+' ' ,z.productype_accounted) >=1   

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Unigrams metadata'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion


--#region ======= Combo Suggestion ===

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Combo search'

truncate table #bbSrchRes

exec DC_SaaSProductTypediscovery_RegularSearch  @maxawc =@maxawc,@UnigramTerm_ID=@UnigramTerm_ID, @rightanchor=0,@skipInnerFilter=1,
																							   @attributeFilter='a.title_type=0 and a.preparedwordcount>1', @skipUnigrams=1,@skipUnigramResultFilter=1,@searchTarget=1
																							   ,@test_searchtarget='Combo'   ,@test_ids=@test_ids,
																							@PNTableName=@TrimmedNgramsTBName,@PNDataTableName=@TrimmedNgramsDataTBName
 
create table #precomboWords(trim_id int ,name nvarchar(450),wordtype int,killedname nvarchar(450),found bit)

-- from UnigramAccounting
insert into #precomboWords(trim_id ,name,wordtype,found)
 select u.id ,a.name ,2,u.ptfound
   from  #unifound u  inner join
		   #attribs a on a.ID=u.uniid    
   where   a.title_type =2
 group by u.id ,a.name,u.ptfound 					   

-- from ModifierAccounting
insert into #precomboWords(trim_id ,name,wordtype,found)
 select u.id ,a.name ,1,u.ptfound
   from  #unifound u  inner join
		   #attribs a on a.ID=u.uniid    
   where   a.title_type =3
 group by u.id ,a.name,u.ptfound

-- collect existing found unigrams

		create table #existswords(trim_id int,aw_id int,wordscount int,wordtype int, name nvarchar(450),foundcount int)
		insert into #existswords(trim_id,aw_id,wordscount,wordtype,name,foundcount)
		select k.*, count(1) over (partition by trim_id,aw_id, wordtype)  
		 from (
						select g.pn_id trim_id ,g.attr_id aw_id, g.wordscount,  m.wordtype , m.name  
						   from #bbSrchRes g cross apply
						            dbo.GetNGramsFromStringStd(g.pnwordprep,1,1,0,0) f inner join
								  #precomboWords m on  g.pn_id =m.trim_id and  f.name =m.name  
						 group by g.pn_id,g.attr_id, g.wordscount,  m.wordtype , m.name  
					) k


 delete from #existswords where wordscount=foundcount
					
						
-- filter words

          update #precomboWords set name=null,killedname= e.name
                  from #precomboWords c inner join
                            #existswords e on  c.trim_id=e.trim_id and c.wordtype=e.wordtype and  e.name=c.name    

 -- build after Parent-Child

  create table #unBf(trim_id int ,name nvarchar(450))
  
  insert into #unBf(trim_id,name)
	select  distinct p.trim_id, p.name 
	   from #stwrds s inner join
	           #precomboWords p on p.trim_id=s.id
	  where p.wordtype =2   and p.found=1   

  insert into #unBf(trim_id,name)
	select  distinct p.trim_id, p.killedname 
	   from #stwrds s inner join
	           #precomboWords p on p.trim_id=s.id
	  where p.wordtype =2  and
				not exists(select 1 from #unBf e where e.trim_id=p.trim_id and e.name=p.killedname)	  

declare @uidxname_unbf varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_unbf+' ON dbo.#unBf (trim_id) include(name)')

  create table #unBfStr(trim_id int ,name nvarchar(4000))

  insert into #unBfStr(trim_id,name)
  select b.trim_id, dbo.StrConcatWithDlm( b.name,' || ' ,1)
    from #unBf b
  group by  b.trim_id  

declare @uidxname_unbf1 varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_unbf1+' ON dbo.#unBfStr (trim_id) include(name)')

update #stwrds set UnigramInProductTypeOrKilled  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id

  truncate table #unBf

  insert into #unBf(trim_id,name)
	select  distinct p.trim_id, p.name 
	   from #stwrds s inner join
	           #precomboWords p on p.trim_id=s.id
	  where p.wordtype =1 and p.found=1

  insert into #unBf(trim_id,name)
	select  distinct p.trim_id, p.killedname 
	   from #stwrds s inner join
	           #precomboWords p on p.trim_id=s.id
	  where p.wordtype =1  and
				not exists(select 1 from #unBf e where e.trim_id=p.trim_id and e.name=p.killedname)	  

  truncate table #unBfStr

  insert into #unBfStr(trim_id,name)
  select b.trim_id, dbo.StrConcatWithDlm( b.name,' || ' ,1)
    from #unBf b
  group by  b.trim_id  

update #stwrds set ModifierInProductTypeOrKilled  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id

  truncate table #unBfStr

  insert into #unBfStr(trim_id,name)
  select p.trim_id, dbo.StrConcatWithDlm( p.name,' || ' ,1)
    from #precomboWords p
    where  p.wordtype =2 and p.found=0     
  group by  p.trim_id  

update #stwrds set UnigramUnaccounted  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id

  truncate table #unBfStr

  insert into #unBfStr(trim_id,name)
  select p.trim_id, dbo.StrConcatWithDlm( p.name,' || ' ,1)
    from #precomboWords p
    where  p.wordtype =1 and p.found=0     
  group by  p.trim_id  

update #stwrds set ModifiersUnaccounted  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id


  truncate table #unBfStr

  insert into #unBfStr(trim_id,name)
  select p.trim_id, dbo.StrConcatWithDlm( p.name,' || ' ,1)
    from #precomboWords p
    where  p.wordtype =2    
  group by  p.trim_id  

update #stwrds set UnigramAccountingAfterPCH  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id

  truncate table #unBfStr

  insert into #unBfStr(trim_id,name)
  select p.trim_id, dbo.StrConcatWithDlm( p.name,' || ' ,1)
    from #precomboWords p
    where  p.wordtype =1    
  group by  p.trim_id  

update #stwrds set ModifierAccountingAfterPCH  =u.name 
        from #stwrds s  inner join
			   #unBfStr u on u.trim_id=s.id

create table #t(id int, combo nvarchar(4000))

insert into #t(id, combo)
exec pt_MakeComboSuggestionBatch
	@selectExpr='select id,pn_trim,productype_accounted,UnigramAccountingAfterPCH,ModifierAccountingAfterPCH from #stwrds',
	@pnTrimName='pn_trim',
	@pnAccountName='productype_accounted',
	@unigAccountName ='UnigramAccountingAfterPCH',
	@modifAccountName='ModifierAccountingAfterPCH',
	@ordName='id'

 update #stwrds set ComboSuggestion =t.combo 
        from #stwrds s inner join
                  #t t on s.id=t.id   

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Combo'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')


--#endregion

--#region       Unigram Groups

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Unigram Groups'

exec DC_SaaSProductTypediscovery_UnigramGroup  @compareWithField='productype_accounted',@title_type=2

if exists(select 1 from #stwrds where UnigramGroup ='')
    exec DC_SaaSProductTypediscovery_UnigramGroup  @compareWithField='producttype_bookend',@title_type=2

if exists(select 1 from #stwrds where UnigramGroup ='')
    exec DC_SaaSProductTypediscovery_UnigramGroup  @compareWithField='ComboSuggestion',@title_type=2

 if exists(select 1 from #stwrds where UnigramGroup ='')
    exec DC_SaaSProductTypediscovery_UnigramGroup  @compareWithField='pn_trim',@title_type=2

 if exists(select 1 from #stwrds where UnigramGroup ='')
    exec DC_SaaSProductTypediscovery_UnigramGroup  @compareWithField='pn_trim',@title_type=3,@title_type_cmp=2

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

  
  			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Unigram groups'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

--#endregion

--#region No Plurals  Set/Kit  /*logic of no plurals is very strange*/

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Set/Kit/Plural'

exec DC_SaaSProductTypediscovery_setKitPlurals 

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Set/Kit'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

--#endregion

--#region  Unigram PT

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Unigram PT'

/* possible will need to add statistics of field UnigramAccounting*/
update #stwrds set producttype_unigram=UnigramAccounting
 from #stwrds s
where productype_accounted ='' and producttype_bookend='' and  
			 productype_accounted_rem = '' and ComboSuggestion='' and
			 ModifierAccounting='' and 
			 UnigramAccounting !='' and CHARINDEX(' || ',UnigramAccounting) =0

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

--#region   Final Produc Types

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Final PT'

    declare @p_f_pt int

    exec @p_f_pt= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Final PT : 1'

create table #fPT(ord int, ProductType nvarchar(450), freq int)

create table #fnlBf(ord int identity, name nvarchar(450),nameSing nvarchar(450))
insert into #fnlBf(name)
select Last3Words from #stwrds
union
select Last2Words from #stwrds
union
select Last1Words from #stwrds
union
select productype_accounted from #stwrds
union
select productype_beforestop from #stwrds
union
select producttype_noPlurals from #stwrds
union
select producttype_bookend from #stwrds
union
select producttype_unigram from #stwrds

update #fnlBf set nameSing=name

      exec dp_Tests_Performance_SetStop @id=@p_f_pt

create table #cvtFnl(ord int, value nvarchar(450))

    exec @p_f_pt= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Final PT : 2'

insert into #cvtFnl(value, ord)
exec [dbo].[RegexpReplaceBatch]	@fieldname ='nameSing',	@identityName ='ord',
																		@dataSource='#fnlBf',
																		@dataSourceAsTable=1,
																		@singlepattern ='[;:\.,_/\\''`\-|]',	@singlereplacewith =' ', 	
																		@patternTable ='', @CaseSensitive =0,@returnAllResult=0

update #fnlBf set nameSing=c.value
   from #fnlBf b inner join
            #cvtFnl c on c.ord=b.ord 

    exec dp_Tests_Performance_SetStop @id=@p_f_pt

truncate table #cvtFnl

    exec @p_f_pt= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Final PT : 3'

insert into #cvtFnl(ord,value)
exec dbo.PluralSingularConvertBatch
      @selectExpr='select nameSing,ord from #fnlBf where nameSing != '''' ' ,
      @fieldname ='nameSing',
      @identityName ='ord',
	  @considerValueAsWord =0, 
	  @toPlurals=0,
	  @useInternalRules=1,
	  @patternsSql='select * from PluralSingularRules',
	  @returnAllResult=0


update #fnlBf set nameSing=c.value
   from #fnlBf b inner join
            #cvtFnl c on c.ord=b.ord 

    exec dp_Tests_Performance_SetStop @id=@p_f_pt

    exec @p_f_pt= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Final PT : 4'

set @uidxname = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname+' ON dbo.#fnlBf (name) include (nameSing)')


declare @sql nvarchar(max)='select b.id, c.nameSing as Last3WordsSrch,
													       c1.nameSing as Last2WordsSrch,
													       c2.nameSing as Last1WordsSrch,
													       c3.nameSing as productype_accountedSrch,
													       c4.nameSing as productype_beforestopSrch,
													       c5.nameSing as producttype_noPluralsSrch,
													       c6.nameSing as producttype_bookendSrch,
														  c7.nameSing as producttype_unigramSrch,
																		b.Last3Words, b.Last2Words, b.Last1Words, b.productype_accounted, b.productype_beforestop,
																		b.producttype_noPlurals, b.producttype_bookend, b.producttype_unigram     
from  #stwrds b  inner join
           #fnlBf c on c.name=b.Last3Words   inner join
           #fnlBf c1 on c1.name=b.Last2Words  inner join
           #fnlBf c2 on c2.name=b.Last1Words inner join
           #fnlBf c3 on c3.name=b.productype_accounted inner join
           #fnlBf c4 on c4.name=b.productype_beforestop inner join
           #fnlBf c5 on c5.name=b.producttype_noPlurals inner join
           #fnlBf c6 on c6.name=b.producttype_bookend inner join
           #fnlBf c7 on c7.name=b.producttype_unigram'


insert into #fPT(ord, ProductType, freq)
exec pt_GetFinalProductTypeBatchEx  @selectFieldsExpr='Last3Words,Last2Words,Last1Words,productype_accounted,productype_beforestop, producttype_noPlurals,producttype_bookend,producttype_unigram',
@selectExpr =@sql,@growScoreFields='Last3Words,Last2Words,Last1Words',
																		 @AUKPTField='productype_accounted', @BEField='producttype_bookend',
																		 @UPTField='producttype_unigram'

 update #stwrds  set FinalProductType =f.ProductType, FinalProductTypeFreq=f.freq   
 from #stwrds s inner join
          #fPT f on f.ord =s.id  

    exec dp_Tests_Performance_SetStop @id=@p_f_pt

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

  
--#region Segment Product Type

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Segment PT'

 update #stwrds SET SegmentNumber=d.val
	   from #stwrds s cross apply
	            (select case when s.productype_accounted=s.productype_beforestop AND s.UnigramAccounting='' AND  s.ModifierAccounting='' AND s.ComboSuggestion='' and s.producttype_bookend='' then 1
													 when s.productype_accounted=s.productype_beforestop AND  s.ComboSuggestion='' and s.producttype_bookend='' then 2
													 when s.productype_accounted=s.productype_beforestop AND  s.ComboSuggestion=''  then 3
													 when  s.ComboSuggestion=''  and s.producttype_bookend='' then 4
													 when  s.ComboSuggestion=''   then 5
													 when  s.productype_accounted=s.productype_beforestop then 6
													else 7 end val
	             )	 d												
        where s.productype_accounted != '' 

 update #stwrds SET SegmentNumber=d.val
	   from #stwrds s cross apply
	            (select case 	 when  ComboSuggestion =''  and producttype_bookendWordsCount >2  then 8.1
								 when  ComboSuggestion =''  and producttype_bookendWordsCount =2  then 8.2
		  						else 9 end val
	             )	 d												
        where s.SegmentNumber=0 and  producttype_bookend !='' 

 update #stwrds SET SegmentNumber=d.val
	   from #stwrds s cross apply
	            (select case when  Last1Words= producttype_unigram  then 10.1
							    when   Last1Words != producttype_unigram  then 10.2
						  end val
	             )	 d												
        where s.SegmentNumber=0 and  producttype_unigram !='' 

 update #stwrds SET SegmentNumber=11
	   from #stwrds s 										
        where s.SegmentNumber=0 and productype_reverse_accounted !='' and productype_accounted_rem=''

 update #stwrds SET SegmentNumber=13
	   from #stwrds s 								
        where s.SegmentNumber=0 and productype_accounted_rem !='' 


 update #stwrds SET SegmentNumber=d.val
	   from #stwrds s cross apply
	            (select case when  Last1Words = UnigramGroup then 14.1
								when  Last1Words != UnigramGroup then 14.2
						end val
	             )	 d												
        where s.SegmentNumber=0 and  UnigramGroup !='' 

/* possible will need to add statistics of field SegmentNumber*/

 update #stwrds SET SegmentProductType=
										  case SegmentNumber
										             when 1 then productype_accounted
													 when 2 then productype_accounted
													 when 3 then productype_accounted
													 when 4 then productype_accounted 
													 when 5 then productype_accounted 
													 when 6 then productype_accounted
													 when 7 then productype_accounted 
													 when  8.1 then producttype_bookend 
													 when  8.2 then producttype_bookend 
													 when  9 then producttype_bookend 
													 when  10.1 then producttype_unigram 
													 when  10.2 then producttype_unigram 
													 when 11 then productype_reverse_accounted 
										/*			 when  12 then ComboSuggestion */
													 when  13 then productype_accounted_rem 
													 when  14.1 then UnigramGroup 
													 when  14.2 then UnigramGroup 
											ELSE '' end		 
         where SegmentNumber>0

declare @uidxname_sgptid varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_sgptid+' ON dbo.#stwrds (SegmentProductType) include(id)')

create table #sgptid_cnt(sg nvarchar(450), cnt int, length int)

insert into #sgptid_cnt(sg, cnt, length)
select s.SegmentProductType,count(s.id), LEN(s.SegmentProductType)
   from #stwrds s  
 where s.SegmentProductType  != ''   
group by s.SegmentProductType   

update #stwrds set Count_segPT=c.cnt,Length_segPT=c.length
    from #sgptid_cnt c inner join
             #stwrds s on c.sg=s.SegmentProductType


 update #stwrds SET SegmentProductType_Confidence=a.ConfidenceAsString, SegmentProductType_Quality=a.QualityAsString
             from #stwrds s inner join
                      AttributesConfidence a on s.SegmentProductType=a.Value 

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Product Type'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')

      /*Wrong place of SPT*/

	   exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_sgptid+'_wp ON dbo.#stwrds (SegmentProductType) include(id,dg_StopWordPos,pn_remainder)')

	 create table #spt_wp(id int)

	 insert into #spt_wp(id)
	   select s.id
		from #stwrds s cross apply
				(select  CHARINDEX(' '+s.SegmentProductType+' ', ' '+s.pn+' ') val) v  cross apply
				dbo.SplitStringToTable(s.dg_StopWords,',',0) spl cross apply
				(select  CHARINDEX(' '+ltrim(rtrim(spl.Part))+' ', ' '+s.pn+' ') val) vsw 
		where s.SegmentProductType !='' and dg_StopWordPos >0 and v.val>vsw.val
		group by s.id

		update #stwrds set SegmentProductType_In_WrongPlace='Yes'
		    from #stwrds s inner join
		             #spt_wp w on s.id=w.id

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

--#endregion

--#region New Segment Product Type of Dan

    exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='SPT for Dan'


update #stwrds set SegmentProductType2Dan = dbo.GetLastWords(s.pn_trim,4),SegmentProductType2TypeDan='last 3 begins with "&" + last 2 = SegPT'
     from #stwrds s 
where s.SegmentProductType= s.Last2Words and left(s.Last3Words,2)='& '

update #stwrds set SegmentProductType2Dan = dbo.GetLastWords(s.pn_trim,4),SegmentProductType2TypeDan='last 3 begins with "and" + last 2 = SegPT'
     from #stwrds s 
where s.SegmentProductType= s.Last2Words and  left(s.Last3Words,4)='and '

update #stwrds set SegmentProductType2Dan = val.v,SegmentProductType2TypeDan='Last word = "kit" + SegPT = word(s) before "kit"'
     from #stwrds s cross apply
             (select s.SegmentProductType+' kit'  v)  val
where s.Last1Words = 'kit' and (val.v= s.Last2Words or val.v= s.Last3Words)

update #stwrds set SegmentProductType2Dan = val.v,SegmentProductType2TypeDan='Last word = "set" + SegPT = word(s) before "set"'
     from #stwrds s cross apply
             (select s.SegmentProductType+' set'  v)  val
where s.Last1Words = 'set' and (val.v= s.Last2Words or val.v= s.Last3Words)

create table #SPTs(id int identity, SPT nvarchar(450),LastWord nvarchar(450), wordsCount int)

insert into #SPTs(SPT,LastWord,wordsCount)
select s.SegmentProductType, dbo.GetLastWords( s.SegmentProductType,1), dbo.GetWordsNumber(s.SegmentProductType)
 from #stwrds s 
where s.SegmentProductType != ''  
group by  s.SegmentProductType

update #stwrds set SegmentProductType_id=pp.id
   from #stwrds s inner join
            #SPTs pp on pp.SPT=s.SegmentProductType  

update #stwrds set SegmentProductType2Dan = nsp.val,
                                  SegmentProductType2TypeDan=case when spt.wc =1 then 'last word is not in SegPT but is common SegPT'
																									  else 'last word is not in SegPT but is common LastWord of SegPT' end		
     from #stwrds s cross apply
             (select dbo.GetWordsNumber (s.SegmentProductType)+1 val) g cross apply
            (select dbo.GetLastWords(s.pn_trim, g.val) val) lw  cross apply
            (select s.SegmentProductType+' '+s.Last1Words val) nsp cross apply
            (select max(w.wordsCount) wc from  #SPTs w where w.id !=s.SegmentProductType_id and w.LastWord=s.Last1Words) spt
where s.SegmentProductType !='' and 
			s.SegmentProductType2Dan = '' and
			 not exists(select 1 from #SPTs w where w.id=s.SegmentProductType_id and w.LastWord=s.Last1Words) and 
			 spt.wc>0 and lw.val= nsp.val
             
update #stwrds SET SegmentProductType2Dan=s.ComboSuggestion,SegmentProductType2TypeDan='Segment 12'
      from #stwrds s
	where s.SegmentProductType2Dan= '' and
			   s. ComboSuggestion != '' and (s.productype_accounted !='' or  s. productype_accounted_rem='')

update #stwrds set SegmentProductType_id=0

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID


   /* Suggest new Product Types by Category field Type */

       exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='SPT by Category'

    declare @CategoryField sysname
    select  top(1) @CategoryField=ColumnName  from dp_TablesColumns where Table_ID =@table_id and RealFieldType_id =201 

    if @CategoryField is not null
     begin
    
       declare @edgePattern nvarchar(100)='[\s,:\.!\\/\(\)]'
       
        create table #cats(id int,fullnode nvarchar(4000),SPT nvarchar(450))
    
	    exec(' insert into #cats(id,fullnode,spt)
				 select  r.force_id_identity,r.['+@CategoryField+'], s.SegmentProductType
					    from '+ @res7+' r inner join
					             #stwrds s on s.id=r.force_id_identity
			       where r.['+@CategoryField+'] !='''' and charindex(''>'', r.['+@CategoryField+']) >1 ')


         create table #nodes_pre_tmp(id int, part nvarchar(450),rowID int)

	     insert into #nodes_pre_tmp(id,part,rowID) 
		select i.id, ltrim(rtrim(j.Part)) part , j.rowID 
		from #cats i cross apply
			   dbo.SplitStringToTable(i.fullnode ,'>',0) j     

          create table #nodes_tmp(id int, part nvarchar(450),rowID int,maxrows int)

	     insert into #nodes_tmp(id,part,rowID,maxrows) 
		select j.id,j.part,j.rowID,MAX(j.rowID) over (partition  by j.id) maxrows
		   from #nodes_pre_tmp j

          create table #nodes(id int, part nvarchar(450),rowID int,maxrows int)

		  insert into #nodes(id,part,rowID,maxrows) 
		  select nt.id ,nt.part,nt.rowID,nt.maxrows
		    from #nodes_tmp nt inner join
		             #cats c on c.id=nt.id and dbo.fn_RegexHaveMatches( nt.part,'(?<=^|'+@edgePattern+')'+dbo.fn_RegexEscape(c.SPT)+'(?=$|'+@edgePattern+')')=1 	  

              create table #nd2sw(nd_id int, sw_id int, part nvarchar(450), nd_maxrows int, nd_row_id int)

		    insert into #nd2sw(nd_id , sw_id , part , nd_maxrows , nd_row_id )
		   select n.id,s.id,n.part,n.maxrows,n.rowID
				    from #nodes n inner join
						   #stwrds s on s.id=n.id and dbo.fn_RegexHaveMatches( s.pn,'(?<=^|'+@edgePattern+')'+dbo.fn_RegexEscape(n.part)+'(?=$|'+@edgePattern+')')=1  

		  create table #n2c_res(id int, part nvarchar(450), reason nvarchar(450)) 
			
			insert into #n2c_res(id , part , reason) 
			select k.id,k.part, case when k.maxrows=k.maxfoundrow then  'Last Node Of Category' else 'Above Last Node Of Category'  end
			 from ( 
						 select  w.nd_id id, w.part, w.nd_maxrows maxrows,w.nd_row_id rowID, max(w.nd_row_id) over (partition by w.nd_id) maxfoundrow
						     from #nd2sw w 
					  ) k where k.rowID=k.maxfoundrow

				    update #stwrds set  SegmentProductType2Category=r.part ,
												SegmentProductType2TypeCategory =r.reason,SegmentProductType2CategoryAcrossID=r.id
					    from #stwrds s inner join
					             #n2c_res r on s.id=r.id
					             
				    create table #accr_res(id int, part nvarchar(450), src_id int)

				    insert into #accr_res(id , part , src_id )
				    select z.id,z.part,z.s2_id
				     from (					             
					  select  s.id ,s2.sw_id s2_id,n.part, max(s2.sw_id) over(partition by s.id) maxs2 
					      from #stwrds s inner join
					               #nodes n on s.id=n.id inner join
					               #nodes n2 on n.part=n2.part  inner join
								#nd2sw s2 on s2.sw_id != s.id and s2.nd_id=n.id 
					   where s.SegmentProductType2CategoryAcrossID=0   
					   ) z where z.s2_id=z.maxs2
					             

				    update #stwrds set SegmentProductType2CategoryAcrossID=a.src_id,SegmentProductType2Category=a.part, SegmentProductType2TypeCategory = 'Across Rows Node Of Category'
				             from #stwrds s inner join
				                      #accr_res a on s.id=a.id 


     end 

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID


--#endregion

--_TS1:  

/* ===   all results of search ===*/

       exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search'

	  declare @p_a_r int

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 1'

delete from #productype_accounted_all_result
          from #productype_accounted_all_result sr inner join
                   #stwrds s on s.id=sr.id and CHARINDEX(' '+sr.expression+' ' , ' '+s.SegmentProductType+' ') >0

      exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 2'

create table #paar_shortest(ord int)

insert into #paar_shortest(ord)
select   sm.ord
  from #productype_accounted_all_result sm inner join
           #productype_accounted_all_result lg on sm.id=lg.id and  sm.ord != lg.ord and sm.expr_length < lg.expr_length and
                                                                                   CHARINDEX(' '+sm.expression+' ',' '+lg.expression+' ') > 0

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 3'

	  declare @uidxname_paar varchar(64) = replace(convert(varchar(64),newid()),'-','_')
	   exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_paar+' ON dbo.#paar_shortest(ord) ')

delete from #productype_accounted_all_result
          from #productype_accounted_all_result sr inner join
                   #paar_shortest s on sr.ord=s.ord

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 4'

update #productype_accounted_all_result set expression_no_punct=dbo.RemoveDuplicates(replace(expression,'-',' '),' ') 

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 5'

update #stwrds set SegmentProductType_no_punct=dbo.RemoveDuplicates(replace(SegmentProductType,'-',' '),' ')  where SegmentProductType !=''

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 6'

delete from #productype_accounted_all_result
          from #productype_accounted_all_result sr inner join
                   #stwrds s on s.id=sr.id and s.SegmentProductType_no_punct=sr.expression_no_punct

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 7'
                   
  /*remainders*/
update #productype_accountedrem_all_result set expression_no_punct=dbo.RemoveDuplicates(replace(expression,'-',' '),' ') 

        exec dp_Tests_Performance_SetStop @id=@p_a_r

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 8'

delete from #productype_accountedrem_all_result
          from #productype_accountedrem_all_result sr inner join
                   #stwrds s on s.id=sr.id and s.SegmentProductType_no_punct=sr.expression_no_punct

        exec dp_Tests_Performance_SetStop @id=@p_a_r

truncate table #paar_shortest

       exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 9'

insert into #paar_shortest(ord)
select   sm.ord
  from #productype_accountedrem_all_result sm inner join
           #productype_accountedrem_all_result lg on sm.id=lg.id and  sm.ord != lg.ord and sm.expr_length < lg.expr_length and
                                                                                   CHARINDEX(' '+sm.expression+' ',' '+lg.expression+' ') > 0

     exec dp_Tests_Performance_SetStop @id=@p_a_r

     exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 10'

delete from #productype_accountedrem_all_result
          from #productype_accountedrem_all_result sr inner join
                   #paar_shortest s on sr.ord=s.ord

     exec dp_Tests_Performance_SetStop @id=@p_a_r

     exec @p_a_r= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Prepare ALL results of search: 11'

	    create table #crasy_bf(id int, expr nvarchar(max))
    
	     insert into #crasy_bf(id, expr)
			     select r.id,  dbo.StrConcatWithDlm(distinct r.expression, @sep_level1,1)
			    from #productype_accounted_all_result  r
			     group by r.id
			     
	    update #stwrds set productype_accounted_all=b.expr
		     from #stwrds s inner join
				    #crasy_bf b on s.id=b.id 
  
	    truncate table #crasy_bf

	     insert into #crasy_bf(id, expr)
			     select r.id,  dbo.StrConcatWithDlm(distinct r.expression, @sep_level1,1)
			    from #productype_accountedrem_all_result  r
			     group by r.id
    
	    update #stwrds set productype_accounted_rem_all=b.expr
		     from #stwrds s inner join
				    #crasy_bf b on s.id=b.id 

     exec dp_Tests_Performance_SetStop @id=@p_a_r

      exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID


--#region  ======= Google Taxonomy ======== 

exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy'

exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Get Rules'

create table #sptRules(objectName nvarchar(450),subjectname nvarchar(450),subjectnamenospace nvarchar(450),
											subjnamepattern nvarchar(450),
											subjnamepatternNoSpace nvarchar(450),	
											rule_id int, rule_type int,
											ord int identity)
											
						-- class rules					
 insert into #sptRules(objectName,subjectname,rule_id,rule_type)
 select a.Name ,asub.Name ,r.id, 1  
  from  pr_Rules r inner join
		   vw_AttrTerm a on a.Cat_ID=@comparableClass_id  and a.id=r.aalObj_id and  a.Type_id=6 inner join
             vw_AttrTerm at on  at.Cat_ID=@comparableClass_id and at.ID=a.SubType_id and at.name='Google Shopping' inner join
             pr_RulesSubjects rs on rs.rules_id=r.id inner join
		   vw_AttrTerm  asub on asub.Cat_ID=@comparableClass_id and asub.id=rs.aalSubj_id and asub.Type_id=6
where r.cat_id=@comparableClass_id and r.SubjectsNumber =1 and r.PositiveSubjectsNumber =1 and r.UnderConstruction=0 

declare @uidxname_sptr varchar(64) = replace(convert(varchar(64),newid()),'-','_')
exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_sptr+' ON dbo.#sptRules(objectName) include(subjectname)')


 declare @SubjAttributeTerm_id int=(select id from rule_Terms where name='Product Type')
 declare @ObjectAttributeTerm_id int=(select id from rule_Terms where name='Google Taxonomy')

                        -- global rules
 insert into #sptRules(objectName,subjectname,rule_id,rule_type)
    select  ot.Name,st.Name,r.id, 2
	from exs_PropertyRules r inner join
		    exs_PropertyRulesSubjects rs on r.id=rs.PropertyRule_id inner join
		    rule_Terms ot on ot.ID=r.ObjAttributeValueTerm_id inner join
		    rule_Terms st on st.ID=rs.SubjAttributeValueTerm_id
	 where r.ObjType_id =6 and r.ObjAttributeTerm_id = @ObjectAttributeTerm_id and
			   rs.SubjType_id =6 and rs.SubjAttributeTerm_id =  @SubjAttributeTerm_id  and
			   not exists(select 1 from #sptRules df where df.objectName=ot.Name and df.subjectname=st.Name)                 

update #sptRules set subjectnamenospace =replace(subjectname ,' ','')

 exec dp_Tests_Performance_SetStop @id=@tmp_perf

exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Singularize'

create table #cvtSubj(value nvarchar(4000),ord int)

insert into #cvtSubj(ord,value)
exec dbo.PluralSingularConvertBatch
      @selectExpr='select subjectname,ord from #sptRules' ,
      @fieldname ='subjectname',
      @identityName ='ord',
	  @considerValueAsWord =0, 
	  @toPlurals=0,
	  @useInternalRules=1,
	  @patternsSql='select * from PluralSingularRules where ruletype !=1'

update #sptRules set subjnamepattern =c.value
        from #sptRules t inner join
                  #cvtSubj c on c.ord =t.ord  

update #sptRules set subjnamepatternNoSpace =replace(subjnamepattern ,' ','')

 exec dp_Tests_Performance_SetStop @id=@tmp_perf

 exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Indexing'

  declare @uidxname_sptrr varchar(64) = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_sptrr+' ON dbo.#sptRules (subjnamepattern) include(objectName)')
  set @uidxname_sptrr = replace(convert(varchar(64),newid()),'-','_')
  exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_sptrr+' ON dbo.#sptRules (subjnamepatternNoSpace) include(objectName)')
  
   exec dp_Tests_Performance_SetStop @id=@tmp_perf

    exec @tmp_perf= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Determine'

    declare @tmp_perf_gtx int

    exec @tmp_perf_gtx= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Determine: 1'

exec DC_SaaSProductTypediscovery_GTX  @srcTable ='#stwrds'  ,@sourceField ='SegmentProductType' ,@srcValueSeparator =';',@comparableClass_id =@comparableClass_id,
																	     @targetField ='PT_GoogleTaxonomy',@runstate=@runstate,@GoogleTaxonomy_Rules='GoogleTaxonomy_Rules'

   exec dp_Tests_Performance_SetStop @id=@tmp_perf_gtx

    exec @tmp_perf_gtx= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Determine: 2'

update #stwrds set GoogleTaxonomy = ltrim(rtrim(f.Part))
    from #stwrds s cross apply
             dbo.SplitStringToTable(s.PT_GoogleTaxonomy,':',0) f 
where  s.PT_GoogleTaxonomy !='' and f.rowID=1              

   exec dp_Tests_Performance_SetStop @id=@tmp_perf_gtx

    exec @tmp_perf_gtx= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Determine: 3'
																	     
exec DC_SaaSProductTypediscovery_GTX  @srcTable ='#stwrds'  ,@sourceField ='productype_accounted_all' ,@srcValueSeparator =';',@comparableClass_id =@comparableClass_id,
																	     @targetField ='GoogleTaxonomy_accounted_all',@runstate=@runstate

   exec dp_Tests_Performance_SetStop @id=@tmp_perf_gtx

    exec @tmp_perf_gtx= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Google taxonomy: Determine: 4'

exec DC_SaaSProductTypediscovery_GTX  @srcTable ='#stwrds'  ,@sourceField ='productype_accounted_rem_all' ,@srcValueSeparator =';',@comparableClass_id =@comparableClass_id,
																	     @targetField ='GoogleTaxonomy_remainders_all',@runstate=@runstate

   exec dp_Tests_Performance_SetStop @id=@tmp_perf_gtx

exec dp_Tests_Performance_SetStop @id=@tmp_perf

 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

			 if isnull(@test_ids,'') != ''
			  exec('insert into #stwrds_test_result
					   select ''Google'' as Operation,''State'' as Iteraton, * from #stwrds where id in ('+@test_ids+') order by id')
              
--#endregion

--#region  ======= gtx Last Node by PT ======== 

declare @MerchantCat nvarchar(300) = (select ColumnName from dp_TablesColumns c where c.Table_ID=@table_id and c.RealFieldType_id=209)  /*Merchant Category*/ 

if @MerchantCat is not null
begin

				exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='gtx Last Node by PT'

                           declare  @sqls nvarchar(1000)='select name,id from exs_ProductTypes'

						  declare @ruleSql  nvarchar(1000)='select ActionType,ExpressionFrom,ExpresstionTo
									from (
												select r.ActionType,r.ExpressionFrom,r.ExpresstionTo,r.ExecuteOrder,r.UploadTargetType    
												   from DataSetPreparationRules r
												  where r.ActionType =0 and r.TransformationForSearch=1
												union   
												select r.ActionType,r.RegexPattern ,r.RegexReplaceExpression ,r.ExecuteOrder,r.UploadTargetType    
												   from DataSetPreparationRules r
												  where r.ActionType =1 and r.TransformationForSearch=1
												union   
												select r.ActionType,'''' RegexPattern , '''' RegexReplaceExpression ,r.ExecuteOrder,r.UploadTargetType    
												   from DataSetPreparationRules r
												  where r.ActionType =2 and r.TransformationForSearch=1
										 ) u where u.UploadTargetType in (6,4) 
										 order by u.ExecuteOrder'

			 create table #tbPTtrans_tmp(value nvarchar(450), ord int)

						  insert into #tbPTtrans_tmp(ord,value)
							exec csr_ChangeStringByRulesBatch @selectExpr=@sqls,
																										@fieldname=  'name',
																										@identityName='id',
																										@rulesSql=@ruleSql,
																										 @onlyProcessed=1		



			 create table #tbPTtrans(valueoriginal nvarchar(450),
			                                                 valuebase nvarchar(450), 
													valuesingular nvarchar(450), 
													cvt_profile nvarchar(450),
													wordscount int,
													 ord int)

                        insert into #tbPTtrans(valueoriginal,valuebase,valuesingular, ord, wordscount,cvt_profile)
				    select z.*,REPLICATE('0',z.wc)
				     from (
							 select p.name,  isnull(t.value,  p.Name) valuebase , isnull(t.value,  p.Name) valuesingular ,p.id, dbo.GetWordsNumber( isnull(t.value,  p.Name)) wc
								from exs_ProductTypes p left join
										#tbPTtrans_tmp t on t.ord=p.id 
							) z	

				    truncate table #cvt_prof

				    insert into #cvt_prof(ord,value,cvt_profile)
				    exec dbo.PluralSingularConvertBatch
						@selectExpr='select valuesingular,ord from #tbPTtrans' ,
						@fieldname ='valuesingular',
						@identityName ='ord',
						 @considerValueAsWord =0, 
						 @toPlurals=0,
						 @useInternalRules=1,
						 @patternsSql='select * from PluralSingularRules where ruletype !=1',
						 @returnAllResult =0,
						 @returnApplicationProfile=1

				update #tbPTtrans set valuesingular =c.value,cvt_profile=c.cvt_profile
					   from #tbPTtrans t inner join
							   #cvt_prof c on c.ord =t.ord  

				create table #nodes_gtx(id int, full_node nvarchar(4000))

				exec(' insert into #nodes_gtx(id, full_node)
							 select force_id_identity, ['+@MerchantCat+']
							 from '+@res7+'
							 where ['+@MerchantCat+'] != '''' and charindex(''>'', ['+@MerchantCat+']) >0 ')

	 	create table #gtx_LastNode(id int, node nvarchar(450), nodetrans nvarchar(450))

				;with cte(id,part,rowID)
				as (
						   select i.id,j.Part,j.rowID   
							   from #nodes_gtx i cross apply
									   dbo.SplitStringToTableLim(i.full_node ,'>',0) j     
					 )
				 , ctem(id,rowid)
				as(
						select  c.id,max(rowID)
						   from cte c
						  group by c.id 
					  )      
				, cter(id,name)
				as(
						 select  c.id,c.part
							  from cte c inner join
									  ctem m on c.id=m.id and c.rowID =m.rowid    
					 )
					 insert into #gtx_LastNode(id,node,nodetrans)
					  select i.id,tr.val,tr.val
						   from cter i cross apply
						            (select left(ltrim(rtrim(i.name)),450) val ) tr


                          set  @sqls ='select node,id from #gtx_LastNode '

			 create table #tbGTXTrans(value nvarchar(450), ord int)

						  insert into #tbGTXTrans(ord,value)
							exec csr_ChangeStringByRulesBatch @selectExpr=@sqls,
																										@fieldname=  'node',
																										@identityName='id',
																										@rulesSql=@ruleSql,
																										 @onlyProcessed=1		

				    update #gtx_LastNode set nodetrans=t.value
				         from #gtx_LastNode ln inner join
					              #tbGTXTrans t on t.ord=ln.id

				create table #pre_grp_node(nodetrans nvarchar(450), id  int identity)

				insert into #pre_grp_node(nodetrans)
				select nodetrans  from #gtx_LastNode  group by nodetrans

				create table #phrasesBf(trim_id int, ph_id int,firstcharpos int, plength int, epos int,numwords int,FoundValue nvarchar(450),ord int  identity)

				insert into #phrasesBf(trim_id , ph_id ,numwords,firstcharpos,FoundValue )
				exec dbo.SearchForNGramsBatch @fieldname='nodetrans',@identityName='id',@sourceSQL='select r.id,r.nodetrans from #pre_grp_node r',
				@ngramsSourceSql='select  id, phrase,PhraseWordsCount  from ProductTypes_KnownPhrases',@ngramsfieldname='phrase', @ngramsidentityName='id',
				@ngramsWordsCountField='PhraseWordsCount',@caseSensitive=0,@returnFoundValue=1,@separators=' -'

				update #phrasesBf set plength=p.PhraseLength,epos=b.firstcharpos+p.PhraseLength
					from #phrasesBf b inner join
							ProductTypes_KnownPhrases p on p.id=b.ph_id

				    declare @uidxname_mc_ph varchar(64) = replace(convert(varchar(64),newid()),'-','_')
				    exec('CREATE NONCLUSTERED INDEX idx_#tr_'+@uidxname_mc_ph+' ON dbo.#phrasesBf(trim_id) include(firstcharpos,epos)')

				 declare @max_wc_pt int = (select max(wordscount) from #tbPTtrans)

                     create table #grp_node(nodetrans nvarchar(450),
													   ngram nvarchar(450),	  
													    ngramsingular nvarchar(450), 
													    cvt_profile nvarchar(450),
													    ord int identity)


				 insert into #grp_node(nodetrans,ngram,ngramsingular,cvt_profile)
				 select u.nodetrans,u.name,u.ngramsingular,u.cvt_profile
				 from (
						   select ndt.id, ndt.nodetrans,n.name,n.name ngramsingular, REPLICATE('0',n.numwords) cvt_profile,n.firstcharpos, n.firstcharpos+len(n.name) lastcharposition
							 from #pre_grp_node ndt cross apply
								    dbo.GetNGramsFromStringStd(ndt.nodetrans,1,@max_wc_pt,0,0) n 
						  ) u
						  where not exists(select 1 from #phrasesBf b where b.trim_id=u.id and b.firstcharpos <= u.firstcharpos and b.epos >= u.lastcharposition)
		--				  order by  nodetrans

				    create table #ngs_grp(ngram nvarchar(450), ngramsingular nvarchar(450),cvt_profile nvarchar(450), ord int identity)

				    insert into #ngs_grp(ngram,ngramsingular,cvt_profile)
				    select ngram,ngram,cvt_profile from #grp_node group by ngram,cvt_profile

				    truncate table #cvt_prof

				    insert into #cvt_prof(ord,value,cvt_profile)
				    exec dbo.PluralSingularConvertBatch
						@selectExpr='select ngram,ord from #ngs_grp' ,
						@fieldname ='ngram',
						@identityName ='ord',
						 @considerValueAsWord =0, 
						 @toPlurals=0,
						 @useInternalRules=1,
						 @patternsSql='select * from PluralSingularRules where ruletype !=1',
						 @returnAllResult =0,
						 @returnApplicationProfile=1

				update #ngs_grp set ngramsingular =c.value,cvt_profile=c.cvt_profile
					   from #ngs_grp t inner join
							   #cvt_prof c on c.ord =t.ord  

				 update #grp_node set ngramsingular = n.ngramsingular, cvt_profile=n.cvt_profile
				          from #grp_node g inner join
						         #ngs_grp n on n.ngram=g.ngram 
				    where g.ngramsingular != n.ngramsingular or g.cvt_profile != n.cvt_profile

				 create table #fnd_gtx(nodetrans nvarchar(450), pt nvarchar(450), pt_singular nvarchar(450), ng_found nvarchar(450),  skipType int)

				 insert into #fnd_gtx(nodetrans,pt,skipType,pt_singular,ng_found)
				 select g.nodetrans,t.valueoriginal, 
							 case when len(t.cvt_profile)=1 then
										  case when left(g.cvt_profile,1) = '0'  and left(t.cvt_profile,1) = '1' then 1 else 0 end
									  else
									    case when left(g.cvt_profile,1) != left(t.cvt_profile,1)  then 1 
									               when  exists(
																	   select 1
																			   from dbo.StringToCharArray(g.cvt_profile) gn inner join
																					   dbo.StringToCharArray(t.cvt_profile)  tn on  gn.rowID=tn.rowID and gn.charelement='0' and tn.charelement='1'
    																    )
							 							    then 1 
												 else 0 
										 end
								end, 
							 t.valuesingular, g.ngram
				   from #grp_node g inner join
						  #tbPTtrans t on t.valuesingular=g.ngramsingular

                   create table #pre_gtx_res(nodetrans nvarchar(450),  foundpt nvarchar(450),   foundpt_singular nvarchar(450), ql nvarchar(20), ng_found nvarchar(450),  ord int identity)

			    insert into #pre_gtx_res(nodetrans,foundpt, foundpt_singular, ql, ng_found)
			    select f.nodetrans,  f.pt, f.pt_singular,
						  isnull(a.QualityAsString,'not found'),
						  f.ng_found
			       from #fnd_gtx f left join
						  AttributesConfidence a on a.Value=f.pt
				where f.skipType =0
				group by f.nodetrans, f.pt , f.pt_singular, f.ng_found, a.QualityAsString
				 order by f.nodetrans,f.pt_singular

				create table #fnd_ord(ord int)

				insert into #fnd_ord(ord)
				select  pch.ord
				    from #pre_gtx_res pp inner join
				             #pre_gtx_res pch on pp.ord != pch.ord and  pp.nodetrans=pch.nodetrans and len(pp.foundpt) > len(pch.foundpt) and 
														    dbo.fn_RegexHaveMatches(pp.foundpt_singular,'(?<=^|\s)'+dbo.fn_RegexEscapeLim(pch.foundpt_singular)/*+'(?=$|\s)'*/)=1

				delete from #pre_gtx_res
				     from #pre_gtx_res p inner join
					          #fnd_ord o on o.ord=p.ord 

                   create table #pre_gtx_rest(id int, foundpt nvarchar(4000), ql nvarchar(4000), freq int, remainder nvarchar(4000))

			    insert into #pre_gtx_rest(id , foundpt, ql, freq,remainder)
			    select ln.id, w.pts,w.qls,w.freq,dbo.RemoveDuplicates(dbo.Trim(dbo.ReplaceString(w.nodetrans,w.ngs,@sep_level1,@sep_level1),@sep_level1 ),' ')
			       from (
							select r.nodetrans, dbo.StrConcatWithDlm(r.foundpt,@sep_level1,0) pts , dbo.StrConcatWithDlm(r.ql,@sep_level1,0) qls, count(1) freq,
									   dbo.StrConcatWithDlm(r.ng_found,@sep_level1,0)  ngs
							   from #pre_gtx_res r 
							group by r.nodetrans 
						 ) w inner join
                              #gtx_LastNode ln on ln.nodetrans=w.nodetrans     

						  update #stwrds set Last_Node_of_MC = ln.node,Last_Node_PT_Skip_Type = case  isnull(g.skipType,0) 
																																							 when 1 then 'Plural/Singular'
																																							 else ''
																																								end,
								     Last_Node_PT_Skipped =isnull(g.pt ,'')
						      from #stwrds s inner join
							          #gtx_LastNode ln on ln.id=s.id  left join
									(select g.nodetrans, dbo.StrConcatWithDlm(g.pt,@sep_level1,0) pt, max(skipType) skipType
									     from #fnd_gtx g where g.skipType >0 group by g.nodetrans ) g on g.nodetrans=ln.nodetrans

						  update #stwrds set	Last_Node_Num_of_PT_found = pr.freq,
										Last_Node_PT_Found = pr.foundpt,
										Last_Node_PT_Quality = pr.ql,Last_Node_PT_Remainder=pr.remainder, Last_Node_PT_Skip_Type='', Last_Node_PT_Skipped=''
						     from #stwrds s inner join
							          #pre_gtx_rest pr on pr.id=s.id


				 exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

 end

--#endregion

/*===== main result =======*/

exec @PERFORMANCE_ID= dp_Tests_Performance_SetStart @table_id=@table_id,@test_id=@test_id,@part_idf='Fix analyzes result'

if @haveTestPN = 0
begin


/*save to reserved field names*/

exec ReservedFieldNames_checkForColumns @tempTablename='#stwrds',@oname=@oname

/*save main result*/

declare @odFld nvarchar(max)=( select dbo.StrConcatWithDlm('['+ColumnName+']',',',0)  from dp_TablesColumns where Table_ID =@table_id )
 
if isnull(@odFld,'') != ''
      set @odFld=','+@odFld

declare @selectexpr nvarchar(max)='s.*'+@odFld
declare @fromexpr nvarchar(max)='#stwrds s inner join '+ @res7+' r on s.id=r.force_id_identity'

exec dp_FixTestResult @TblName=@TblName,@spname=@oname, @selectexpr=@selectexpr,@fromexpr=@fromexpr,
@orderby='s.Count_segPT desc, s.SegmentProductType, s.count1 desc, s.Last1Words, s.count2 desc, s.Last2Words, s.count3 desc, s.Last3Words'


/* save SPT History*/

delete from SPTHistory where DataSet_id=@table_id

insert into SPTHistory(DataSet_id,DataSet_Name,Product_ID,SegmentNumber,SegmentProductType,TrimmedPN)
select @table_id,@TblName,s.id,s.SegmentNumber,s.SegmentProductType,s.pn_trim
  from #stwrds s
where s.SegmentProductType !=''

declare @gtxField sysname = (select top(1) tc.ColumnName from dp_TablesColumns tc where tc.Table_ID=@table_id and tc.RealFieldType_id=2000)

if @gtxField is not null
  begin

   exec(  'update SPTHistory set GoogleID= gtx.GoogleID
				from SPTHistory sh inner join
				          '+ @res7+' r on  sh.DataSet_id='+ @table_id+' and sh.Product_ID=r.force_id_identity inner join
						GoogleTaxonomy gtx on gtx.Name= r.['+@gtxField+']')


  end



end

declare @phtb nvarchar(1000)= dbo.fn_GetMainResultTableName(@table_id, @test_id,1)+'_20'

if OBJECT_ID(@phtb,'U') is not null
   exec('drop table '+@phtb)
  
  exec('select * into '+@phtb+' from #foundPhrasesForTitles')

/*Analysis diagnostic data*/
create table #test_diagnostic_data(CategoryFieldExists bit)

insert into #test_diagnostic_data(CategoryFieldExists)
values(case when @CategoryField is not null then 1 else 0 end)

set @phtb = dbo.fn_GetMainResultTableName(@table_id, @test_id,1)+'_9999'

if OBJECT_ID(@phtb,'U') is not null
   exec('drop table '+@phtb)
  
  exec('select * into '+@phtb+' from #test_diagnostic_data')

   exec dp_Tests_Performance_SetStop @id=@PERFORMANCE_ID

/*test statistics*/

if isnull(@test_ids,'') != ''
begin

  declare @res_test nvarchar(2000)=dbo.fn_GetMainResultTableName(@table_id,@test_id,1)+'_1000'
  
  if OBJECT_ID(@res_test,'U')  is not null
     exec('drop table '+@res_test)
  
   exec('select * into '+@res_test+' from #stwrds_test_result')

  set @res_test =dbo.fn_GetMainResultTableName(@table_id,@test_id,1)+'_1001'
  
  if OBJECT_ID(@res_test,'U')  is not null
     exec('drop table '+@res_test)
  
   exec('select * into '+@res_test+' from #srchResult_test')

  set @res_test =dbo.fn_GetMainResultTableName(@table_id,@test_id,1)+'_1002'
  
  if OBJECT_ID(@res_test,'U')  is not null
     exec('drop table '+@res_test)
  
   exec('select * into '+@res_test+' from #bbSrchRes_test')

end

END



