WITH cases AS(
    SELECT
        id AS Case_ID__c,
        casenumber
    FROM
        datalake_raw.salesforce_cases
),
mortgage_valuations AS(
    SELECT
        applicantname AS Contact_Name__c,
        valuationContactEmail AS Contact_Email__c,
        valuationTelNumber AS Contact_PhoneNumber__c,
        ageMain AS Property_Age__c,
        '' AS Expected_CompletionDate__c,
        flatsType AS Property_Type__c,
        rooms_bedrooms AS Bedroom_Count__c,
        '' AS Standard_Construction__c,
        tenure_type AS Ownership_Type__c,
        tenure_terms AS Remaining_Lease_Years__c,
        '' AS Is_Ex_Local_Authority__c,
        overNonResidential AS Located_above_business__c,
        '' AS Business_Nature__c,
        case_number AS Case_Number__c,
        propertyAddress AS Property_Address__c,
        valuationDate AS Valuation_Date__c,
        valuations_marketValue AS Market_Value__c,
        valuations_insuranceReinstatementEstimate AS Insurance_Reinstatement_Estimate__c,
        valuations_marketRentPerMonth AS Market_Rent_per_month__c,
        essentialRecommendedRepairs AS Essential_Repairs__c,
        floodingRisk_seaOrRiver AS Flooding_risk_sea_or_river__c,
        floodingRisk_surfaceWater AS Flooding_Risk_surface_water__c,
        invasiveSpecies AS Invasive_Species__c,
        overheadPowerLines AS Overhead_Power_Lines__c,
        tenure_type AS Tenure__c,
        tenure_terms AS Terms_Lease_Details__c,
        serviceCharge AS Service_Charge_per_annum__c,
        fitForImmediateOccupation AS Is_the_property_fit_for_immediate_occupa__c,
        isReinspectionRequired AS Is_a_Reinspection_Required__c,
        originalBuilding AS Original_Building_construction_type__c,
        condition_external AS External_Condition__c,
        condition_internal AS Internal_Condition__c,
        isPropertyLet AS Is_the_property_let__c
    FROM
        datalake_raw.mortgage_valuations
)
SELECT
    v.Contact_Name__c,
    v.Contact_Email__c,
    v.Contact_PhoneNumber__c,
    v.Property_Age__c,
    v.Expected_CompletionDate__c,
    v.Property_Type__c,
    v.Bedroom_Count__c,
    v.Standard_Construction__c,
    v.Ownership_Type__c,
    v.Remaining_Lease_Years__c,
    v.Is_Ex_Local_Authority__c,
    v.Located_above_business__c,
    v.Business_Nature__c,
    v.Case_Number__c,
    v.Property_Address__c,
    v.Valuation_Date__c,
    v.Market_Value__c,
    v.Insurance_Reinstatement_Estimate__c,
    v.Market_Rent_per_month__c,
    v.Essential_Repairs__c,
    v.Flooding_risk_sea_or_river__c,
    v.Flooding_Risk_surface_water__c,
    v.Invasive_Species__c,
    v.Overhead_Power_Lines__c,
    v.Tenure__c,
    v.Terms_Lease_Details__c,
    v.Service_Charge_per_annum__c,
    v.Is_the_property_fit_for_immediate_occupa__c,
    v.Is_a_Reinspection_Required__c,
    v.Original_Building_construction_type__c,
    v.External_Condition__c,
    v.Internal_Condition__c,
    v.Is_the_property_let__c,
    c.Case_ID__c
FROM
    mortgage_valuations v
    LEFT JOIN cases c ON v.Case_Number__c = c.casenumber
WHERE
    c.Case_ID__c IS NOT NULL
