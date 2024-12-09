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

Company table:
    id
    name
    jobs (one to many)

Skills:
    id
    name
