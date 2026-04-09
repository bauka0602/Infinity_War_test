def default_store():
    return {
        "users": [
            {
                "email": "admin@kazatu.edu.kz",
                "password": "admin123",
                "displayName": "System Admin",
                "role": "admin",
                "token": "seed-admin-token",
            },
        ],
        "courses": [],
        "teachers": [],
        "rooms": [],
        "groups": [
            {
                "name": "БИ-101",
                "student_count": 24,
                "has_subgroups": 0,
                "language": "ru",
                "study_course": 1,
            },
            {
                "name": "БИ-102",
                "student_count": 22,
                "has_subgroups": 0,
                "language": "kk",
                "study_course": 1,
            },
            {
                "name": "БИ-201",
                "student_count": 26,
                "has_subgroups": 1,
                "language": "ru",
                "study_course": 2,
            },
            {
                "name": "БИ-301",
                "student_count": 25,
                "has_subgroups": 1,
                "language": "ru",
                "study_course": 3,
            },
            {
                "name": "БИ-401",
                "student_count": 23,
                "has_subgroups": 0,
                "language": "ru",
                "study_course": 4,
            },
            {
                "name": "ИС-201",
                "student_count": 21,
                "has_subgroups": 0,
                "language": "kk",
                "study_course": 2,
            },
        ],
        "schedules": [],
        "sections": [],
    }
