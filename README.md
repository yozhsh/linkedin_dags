# Train airflow skills
# data set https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024


Job table:
    id
    title
    link unique=True
    uuid index=True
    job_location
    search_city
    (job_level)level
    (job_type)type
    (job_summary)summary(text)
    company_table_id

Company table:
    id
    name

Skills:
    id
    name unique

SkillsToJob:
    id
    skill_id
    job_id
