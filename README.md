## HOW TO USE MORNING STAR SCRAPER?

The scraper is in a "scraper" folder with the file name "morning_star_pipeline". <br />
Example code to use the Morning Star pipeline : 

```python
from scraper.morning_star_pipeline import MorningStarPipeline

pipeline = MorningStarPipeline(industry="", rating="", filter="", amount=10, country=["Indonesia"])
results = pipeline.get()
```
For use the pipeline, you must input the parameter :

1. **industry**       : input for industry categories, you can input categories like Banks, Automobile, Aerospace, etc. for detail of industry, you can check filter in the MS Website on link : https://www.sustainalytics.com/esg-rating

2. **rating**          : Input for ESG Score rating. ESG Score is divided into several categories.
                  <br />0: Negligible (0 - 10)
                  <br />1: Low (10 - 20)
                  <br />2: Medium (20 - 30)
                  <br />3: High (30 - 40)
                  <br />4: Severe (40+) <br /> To use the rating filter, you must input the number of rating, like 0, 1, 3, 4.

3. **name**            : Input for company's name. you can input names like PT Vale Indonesia Tbk, 11 Bit Studios SA, etc.
                  <br /> You also can input the ticker like JKT:INCO, NAS:FLWS, etc.

4. **amount**         : input for the amount of data you want to retrieve, like 10, 20, 30, 1000, etc

5. **country**          : Input for certain countries, like Indonesia, Singapore, Malaysia

*Note*             : For retrieve all data on MS. you must input industry, rating, and filter with "" (empty string, not whitespace) and country with [] (empty-list)

### what data will be retrieved by the pipeline?

The morning star pipeline will return several data for ESG Score. List of data from the pipeline :
1. **symbol** : The identifier of the company, such as INCO.JKT, AALI.JKT, etc
2. **last_esg_update_date**: This date indicates when the company research was updated for any reason other than completing its annual report cycle. E.g., Sub-industry assessment, news monitoring event, change to metrics/indicators. (Sources : Morning Star)
3. **esg_score** : ESG refers to the three central factors of measuring the impact of sustainability and ethics in making investment decisions.  (Sources : IDX)
4. **controversy_risk** : The Controversy Rating reflects a company’s level of involvement in issues and how it manages these issues. (Sources :Morning Star)
5. **environment_risk_score** : The environmental risk represents the unmanaged environmental risk exposure after taking into account a company’s management of such risks. (Sources : Morning Star)
6. **social_risk_score** : The social risk represents unmanaged social risk exposure after taking into account a company’s management of such risks. (Sources : Morning Star)
7. **governance_risk_score** : The governance risk represents the unmanaged governance risk exposure after taking into account a company's management of such risks. (Sources : Morning Star)
8. **updated on** : The time when the data is updated in the database.
9. **industry_rank** : The company ranks in certain industry categories in Morning Star.
10. **total_company_on_industry** : The total number of companies in certain industry categories.
11. **global_rank** : The company ranks in worldwide
12. **total_companies_on_ms** : The total number of companies in the Morning Star database.
13. **last_esg_full_update** : This date indicates when a company went through a full annual assessment. (Sources :Morning Star)
14. **industry_group** : The Name of the industry category, like banks, Insurance, etc

*Note* : You have to be careful for NULL value on data. if the data doesn't exist, the data will return NULL value on the column.

For rank of company in industry categories, you can use: <br />
```
industry_rank out of total_company_on_industry
```

For global rank of company in worldwide, you can use: <br />
```
global_rank out of total_companies_on_ms
```

#### Happy Coding All !!!
