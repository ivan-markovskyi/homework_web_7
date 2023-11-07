from sqlalchemy import func, desc, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    result = (
        session.query(
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Student)
        .join(Grade)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(5)
        .all()
    )
    return result


def select_02():
    result = (
        session.query(
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .filter(Grade.subjects_id == 1)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(1)
        .all()
    )
    return result


def select_03():
    result = (
        session.query(
            Group.id,
            Group.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Group)
        .join(Student)
        .join(Grade)
        .join(Subject)
        .filter(Subject.id == 1)
        .group_by(Group.id)
        .order_by(desc("average_grade"))
        .all()
    )

    return result


def select_04():
    result = (
        session.query(
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .all()
    )
    return result


def select_05():
    result = (
        session.query(
            Teacher.fullname.label("teacher"),
            Subject.name,
        )
        .select_from(Teacher)
        .join(Subject)
        .filter(Teacher.id == 1)
        .all()
    )

    return result


def select_06():
    result = (
        session.query(
            Group.name,
            Student.fullname,
        )
        .select_from(Student)
        .join(Group)
        .filter(Group.id == 1)
        .all()
    )

    return result


def select_07():
    result = (
        session.query(
            Student.id,
            Student.fullname,
            Grade.grade,
        )
        .select_from(Group)
        .join(Student)
        .join(Grade)
        .join(Subject)
        .filter(and_(Subject.id == 1, Group.id == 1))
        .all()
    )

    return result


def select_08():
    result = (
        session.query(
            Teacher.id,
            Teacher.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Teacher)
        .join(Subject)
        .join(Grade)
        .filter(Teacher.id == 1)
        .group_by(Teacher.id)
        .all()
    )

    return result


def select_09():
    result = (
        session.query(
            Subject.id.distinct(),
            Subject.name,
        )
        .select_from(Subject)
        .join(Grade)
        .join(Student)
        .filter(Student.id == 1)
        .all()
    )

    return result


def select_10():
    result = (
        session.query(
            Subject.id.distinct(),
            Subject.name,
        )
        .select_from(Teacher)
        .join(Subject)
        .join(Grade)
        .join(Student)
        .filter(and_(Student.id == 1, Teacher.id == 2))
        .all()
    )

    return result


if __name__ == "__main__":
    print(select_10())
