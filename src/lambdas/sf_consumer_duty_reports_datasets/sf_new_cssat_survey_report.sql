with surveyquestionresponse as (
    select survey_taker__c,
        question_type__c,
        name,
        response__c,
        survey_question__c,
        response_numeric__c,
        createddate
    from salesforce_surveyquestionresponse
),
surveytaker as (
    Select id,
        name,
        survey__c
    from salesforce_surveytaker
),
survey as (
    Select id,
        name
    from salesforce_survey
),
surveyquestion as (
    Select id,
        name
    from salesforce_surveyquestion
)
SELECT st.name as survey_response,
    sqr.name as response_id,
    sqr.question_type__c as question_type,
    sq.name as name,
    sqr.response__c as response,
    sqr.response_numeric__c as response_numeric,
    sqr.createddate as created_date
FROM surveyquestionresponse sqr
    inner join surveytaker st on (sqr.survey_taker__c = st.id)
    inner join surveyquestion sq on (sqr.survey_question__c = sq.id)
    inner join survey s on (s.id = st.survey__c)
where s.name = 'How was your recent experience?'
