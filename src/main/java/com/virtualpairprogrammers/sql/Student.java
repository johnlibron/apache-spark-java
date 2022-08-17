package com.virtualpairprogrammers.sql;

public class Student {

    private Long studentId;

    private Long examCenterId;

    private String subject;

    private Integer year;

    private Integer quarter;

    private Long score;

    private String grade;

    public Long getStudentId() {
        return studentId;
    }

    public void setStudentId(Long studentId) {
        this.studentId = studentId;
    }

    public Long getExamCenterId() {
        return examCenterId;
    }

    public void setExamCenterId(Long examCenterId) {
        this.examCenterId = examCenterId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getQuarter() {
        return quarter;
    }

    public void setQuarter(Integer quarter) {
        this.quarter = quarter;
    }

    public Long getScore() {
        return score;
    }

    public void setScore(Long score) {
        this.score = score;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }
}
