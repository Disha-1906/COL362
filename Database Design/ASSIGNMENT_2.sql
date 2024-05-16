CREATE TABLE DEPARTMENT(
    dept_id CHAR(3) PRIMARY KEY,
    dept_name VARCHAR(40) NOT NULL UNIQUE
);
CREATE TABLE VALID_ENTRY(
    dept_id CHAR(3),
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL,
    FOREIGN KEY (dept_id) references DEPARTMENT(dept_id) ON UPDATE CASCADE
);
CREATE TABLE STUDENT(
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) NOT NULL PRIMARY KEY,
    address VARCHAR(100),
    contact_number CHAR(10) NOT NULL UNIQUE,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INTEGER NOT NULL CHECK (tot_credits>=0),
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) references DEPARTMENT(dept_id) ON UPDATE CASCADE
);
CREATE OR REPLACE FUNCTION check_valid_course(course_id char, dept_id char) RETURNS BOOLEAN AS $$
BEGIN
    RETURN SUBSTRING(course_id FROM 4 FOR 3) ~ '^[0-9][0-9][0-9]$' AND 
           dept_id = SUBSTRING(course_id FROM 1 FOR 3);
END;
$$ LANGUAGE plpgsql;
CREATE TABLE COURSES(
    course_id CHAR(6) PRIMARY KEY NOT NULL CHECK (check_valid_course(course_id,dept_id)),
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits>0),
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) references DEPARTMENT(dept_id) ON UPDATE CASCADE
);
CREATE TABLE PROFESSOR(
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER,
    resign_year INTEGER,
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) references DEPARTMENT(dept_id) ON UPDATE CASCADE,
    CHECK (start_year is NULL or resign_year is NULL or start_year <= resign_year)
);
CREATE TABLE COURSE_OFFERS(
    course_id CHAR(6),
    session VARCHAR(9) ,
    semester INTEGER NOT NULL CHECK (semester IN (1, 2)),
    professor_id VARCHAR(10),
    capacity INTEGER,
    enrollments INTEGER,
    FOREIGN KEY (course_id) references COURSES(course_id) ON UPDATE CASCADE,
    FOREIGN KEY (professor_id) references PROFESSOR(professor_id),
    PRIMARY KEY (course_id, session, semester)
);
CREATE TABLE STUDENT_COURSES(
    student_id CHAR(11),
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER CHECK (semester IN (1, 2)),
    grade NUMERIC NOT NULL CHECK (grade >= 0 AND grade <= 10),
    FOREIGN KEY (student_id) references STUDENT(student_id) on UPDATE CASCADE,
    FOREIGN KEY (course_id, session, semester) references COURSE_OFFERS(course_id, session, semester) ON DELETE CASCADE ON UPDATE CASCADE
);


-- 2.1.1
CREATE OR REPLACE FUNCTION validate_student_id() RETURNS trigger AS $$
BEGIN
    IF (LENGTH(NEW.student_id) <> 10) THEN RAISE EXCEPTION 'INVALID'; END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM valid_entry ve
        WHERE ve.entry_year = CAST(SUBSTRING(NEW.student_id,1,4) AS INTEGER)
        AND ve.dept_id = SUBSTRING(NEW.student_id,5,3)
        AND ve.seq_number = CAST(SUBSTRING(NEW.student_id,8,3) AS INTEGER )
    ) THEN
    RAISE EXCEPTION 'INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER validate_student_id
BEFORE INSERT ON student
FOR EACH ROW EXECUTE PROCEDURE validate_student_id();


-- 2.1.2
CREATE OR REPLACE FUNCTION update_seq_number() RETURNS trigger AS $$
BEGIN
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE entry_year = CAST(SUBSTRING(NEW.student_id , 1, 4) AS INTEGER)
    AND dept_id = SUBSTRING(NEW.student_id, 5, 3);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_seq_number
AFTER INSERT ON student
FOR EACH ROW EXECUTE PROCEDURE update_seq_number();


-- 2.1.3
CREATE OR REPLACE FUNCTION validate_email_id() RETURNS TRIGGER AS $$
BEGIN
  IF NOT (SUBSTRING(NEW.email_id, 1, 10) = NEW.student_id
  AND SUBSTRING(NEW.email_id, 11, 1) = '@'
  AND SUBSTRING(NEW.email_id, 12, 3) = NEW.dept_id
  AND SUBSTRING(NEW.email_id, 15, 11) = '.iitd.ac.in')
  THEN
  RAISE EXCEPTION 'INVALID';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER validate_email_id
BEFORE INSERT ON student
FOR EACH ROW EXECUTE PROCEDURE validate_email_id();

CREATE TABLE student_dept_change(
    old_student_id CHAR(11),
    old_dept_id CHAR(3),
    new_dept_id CHAR(3),
    new_student_id CHAR(11),
    FOREIGN KEY (old_dept_id) references DEPARTMENT(dept_id),
    FOREIGN KEY (new_dept_id) references DEPARTMENT(dept_id)
);


-- 2.1.4
CREATE OR REPLACE FUNCTION validate_dept_change_1() RETURNS TRIGGER AS $$
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM department WHERE dept_id = NEW.dept_id) THEN RAISE EXCEPTION 'INVALID';
    END IF;
    IF (EXISTS(
        SELECT 1
        FROM student_dept_change sc
        WHERE sc.new_student_id = OLD.student_id
    )) THEN RAISE EXCEPTION 'Department can be changed only once';
    END IF;
    IF (CAST(SUBSTRING(OLD.student_id,1,4) AS INTEGER) < 2022)
    THEN RAISE EXCEPTION 'Entry year must be >= 2022';
    END IF;
    IF( ((
        SELECT avg(s.grade)
        FROM student_courses s
        WHERE s.student_id =  OLD.student_id
    ) <= 8.5) or ((SELECT tot_credits FROM student WHERE student_id = OLD.student_id)=0))
    THEN RAISE EXCEPTION 'Low Grade';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER  log_student_dept_change_1
BEFORE UPDATE ON student
FOR EACH ROW WHEN (NEW.dept_id <> OLD.dept_id)
EXECUTE PROCEDURE validate_dept_change_1();

CREATE OR REPLACE FUNCTION validate_dept_change_2() RETURNS TRIGGER AS $$
BEGIN 
    INSERT INTO student_dept_change values (OLD.student_id, OLD.dept_id, NEW.dept_id, 
    (SELECT CONCAT(SUBSTRING(OLD.student_id,1,4),dept_id,LPAD(CAST(seq_number as CHAR(3)),3,'0')) FROM valid_entry WHERE entry_year=CAST(SUBSTRING(OLD.student_id,1,4) AS INTEGER) AND dept_id=NEW.dept_id));

    UPDATE student SET 
    student_id = (SELECT CONCAT(SUBSTRING(OLD.student_id,1,4),dept_id,LPAD(CAST(seq_number as CHAR(3)),3,'0')) FROM valid_entry WHERE entry_year=CAST(SUBSTRING(OLD.student_id,1,4) AS INTEGER) AND dept_id=NEW.dept_id),
    email = (SELECT CONCAT(SUBSTRING(OLD.student_id,1,4),LPAD(CAST(seq_number as CHAR(3)),3,'0'),'@',dept_id, '.iitd.ac.in') FROM valid_entry WHERE entry_year=CAST(SUBSTRING(OLD.student_id,1,4) AS INTEGER) AND dept_id=NEW.dept_id) 
    WHERE student_id = OLD.student_id;

    UPDATE valid_entry SET seq_number = seq_number + 1 WHERE entry_year = CAST(SUBSTRING(OLD.student_id,1,4) AS INTEGER) AND dept_id = NEW.dept_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER  log_student_dept_change_2
AFTER UPDATE ON student
FOR EACH ROW WHEN (NEW.dept_id <> OLD.dept_id)
EXECUTE PROCEDURE validate_dept_change_2();


-- 2.2.1
CREATE MATERIALIZED VIEW course_eval as 
SELECT course_id, session, semester, count(student_id) as number_of_students, avg(grade) as average_grade, max(grade) as max_grade, min(grade) as min_grade
FROM student_courses 
GROUP BY (course_id, session, semester);

CREATE OR REPLACE FUNCTION update_eval() RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_eval
AFTER INSERT OR UPDATE OR DELETE ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE update_eval();


-- 2.2.2
CREATE FUNCTION update_total() RETURNS TRIGGER AS $$
BEGIN
    UPDATE student
    SET tot_credits = (SELECT credits FROM courses where courses.course_id = NEW.course_id) + tot_credits
    WHERE student_id = NEW.student_id;
  RETURN NULL; 
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_total
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE update_total();


-- 2.2.3
CREATE FUNCTION check_course_credit() RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT tot_credits FROM student WHERE student_id = NEW.student_id) + (SELECT credits FROM courses WHERE course_id = NEW.course_id)>60
    THEN RAISE EXCEPTION 'Invalid';
    END IF;
    IF (SELECT count(course_id) FROM student_courses GROUP BY (student_id, session, semester) HAVING student_id = NEW.student_id AND session = NEW.session AND semester = NEW.semester) >= 5
    THEN RAISE EXCEPTION 'Invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_course_credit
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE check_course_credit();


-- 2.2.4
CREATE FUNCTION check_5() RETURNS TRIGGER AS $$
BEGIN
    IF (((SELECT credits FROM courses WHERE course_id = NEW.course_id) = 5) 
            AND 
        (SUBSTRING(NEW.student_id,1,4) <> SUBSTRING(NEW.session,1,4)))
    THEN RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_5
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE check_5();


-- 2.2.5
CREATE MATERIALIZED VIEW student_semester_summary AS 
SELECT s.student_id,s.session,s.semester, 
       SUM(CASE WHEN grade>=5.0 THEN grade*credits ELSE 0 END)/SUM(CASE WHEN grade>=5.0 THEN credits ELSE 0 END) AS sgpa,
       SUM(CASE WHEN grade>=5.0 THEN credits ELSE 0 END) AS credits
FROM student_courses s join courses on s.course_id = courses.course_id 
GROUP BY (s.student_id,s.session,s.semester);

CREATE FUNCTION insert_student_summary() RETURNS TRIGGER AS $$
BEGIN
    IF(((SELECT credits FROM courses WHERE courses.course_id = NEW.course_id) + (SELECT SUM(c.credits) FROM student_courses s join courses c on s.course_id = c.course_id WHERE student_id = NEW.student_id AND session = NEW.session AND semester = NEW.semester))>26) THEN
    RAISE EXCEPTION 'INVALID';  
    END IF; 
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER insert_student_summary
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE insert_student_summary();

CREATE FUNCTION update_student_summary() RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_student_summary
AFTER UPDATE ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE update_student_summary();

CREATE FUNCTION delete_student_summary() RETURNS TRIGGER AS $$
BEGIN
    UPDATE student SET tot_credits = tot_credits - (SELECT credits FROM courses WHERE course_id = OLD.course_id) WHERE student_id = OLD.student_id;
    UPDATE course_offers SET enrollments = enrollments - 1 WHERE course_id = OLD.course_id AND session = OLD.session AND semester = OLD.semester;
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER delete_student_summary
AFTER delete ON student_courses
FOR EACH ROW
EXECUTE PROCEDURE delete_student_summary();

-- 2.2.6
create or replace function check_capacity_bi_fun() returns trigger as $$
begin
    IF(
        (select enrollments from course_offers where course_id=new.course_id and session=new.session and semester=new.semester)
        >=(select capacity from course_offers where course_id=new.course_id and session=new.session and semester=new.semester))
    then
        raise exception 'course is full';
    END IF;
    update course_offers
    set enrollments=enrollments+1
    where course_id=new.course_id and session=new.session and semester=new.semester;
    return new;
end;
$$ language plpgsql;
create trigger check_capacity_bi
before insert on student_courses
for each row
execute function check_capacity_bi_fun();


-- 2.3.1
CREATE FUNCTION update_course_offers() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN 
        IF NOT EXISTS (SELECT 1 FROM courses WHERE course_id = NEW.course_id) 
        THEN RAISE EXCEPTION 'invalid';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM professor WHERE professor_id = NEW.professor_id)
        THEN RAISE EXCEPTION 'invalid';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER update_course_offers
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE PROCEDURE update_course_offers();


-- 2.3.2
CREATE FUNCTION check_prof_validity() RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT count(course_id) FROM course_offers GROUP BY (professor_id, session) HAVING professor_id = NEW.professor_id AND session = NEW.session) >=4
    THEN RAISE EXCEPTION 'invalid';
    END IF;
    IF (CAST(SUBSTRING(NEW.session,6,4) AS INTEGER)) > (SELECT resign_year FROM professor WHERE professor_id = NEW.professor_id)
    THEN RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_prof_validity
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE PROCEDURE check_prof_validity();

-- 2.4.1
-- %%sql
-- CREATE FUNCTION dept_table_change() RETURNS TRIGGER AS $$
-- BEGIN
--     IF tg_op='update' then
--         IF(old.dept_id<>new.dept_id) then
--             ALTER TABLE student DISABLE TRIGGER ALL;
--             ALTER TABLE courses DISABLE TRIGGER ALL;
--             ALTER TABLE student_courses DISABLE TRIGGER ALL;
--             ALTER TABLE professor DISABLE TRIGGER ALL;
--             ALTER TABLE valid_entry DISABLE TRIGGER ALL;
--             ALTER TABLE student_dept_change DISABLE TRIGGER ALL;
--             UPDATE courses
--             SET course_id=concat(new.dept_id,substring(course_id from 4))
--             WHERE left(course_id,3)=old.dept_id;
--             UPDATE course_offers
--             SET course_id=concat(new.dept_id,substring(course_id from 4))
--             WHERE left(course_id,3)=old.dept_id;
--             UPDATE student_courses
--             SET course_id=concat(new.dept_id,substring(course_id from 4))
--             WHERE left(course_id,3)=old.dept_id;
--             UPDATE professor
--             SET dept_id=new.dept_id
--             WHERE dept_id=old.dept_id;
--             UPDATE student
--             SET student_id=concat(substring(student_id from 1 for 4),new.dept_id,substring(student_id from 8)),
--                 email_id=concat(substring(student_id from 1 for 4),new.dept_id,substring(student_id from 8),'@',new.dept_id,'.iitd.ac.in'),
--                 dept_id=new.dept_id
--             WHERE dept_id=old.dept_id;
--             ALTER TABLE student enable trigger all;
--             ALTER TABLE courses enable trigger all;
--             ALTER TABLE student_courses enable trigger all;
--             ALTER TABLE professor enable trigger all;
--             ALTER TABLE valid_entry enable trigger all;
--             ALTER TABLE student_dept_change enable trigger all;
--         END IF;
--     elsIF tg_op='delete' then
--         IF exists(select 1 from student WHERE dept_id=old.dept_id)
--         then
--             raise exception 'Department has students';
--         END IF;
--         DELETE FROM professor WHERE dept_id=old.dept_id;
--         DELETE FROM course_offers WHERE left(course_id,3)=old.dept_id;
--         DELETE FROM courses WHERE left(course_id,3)=old.dept_id and dept_id=old.dept_id;
--     END IF;
--     return old;
-- end;
-- $$ language plpgsql;
-- create trigger update_dept_id
-- before delete or UPDATE on department
-- for each row
-- execute function dept_table_change();