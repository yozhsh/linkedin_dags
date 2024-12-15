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
    got_summary boolean
    summary_table_id
    company_table_id

Summary table:
    id
    name

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
