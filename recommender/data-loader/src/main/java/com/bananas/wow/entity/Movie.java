package com.bananas.wow.entity;

/**
 * 1^Toy Story (1995)^ ^81 minutes^March 20, 2001^1995^English ^Adventure|Animation|Children|Comedy|Fantasy ^Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn ^John Lasseter
 */

public class Movie {

	public Movie(Integer mid, String name, String descri, String timelong, String issue, String shoot, String language, String genres, String actors, String directors) {
		this.mid = mid;
		this.name = name;
		this.descri = descri;
		this.timelong = timelong;
		this.issue = issue;
		this.shoot = shoot;
		this.language = language;
		this.genres = genres;
		this.actors = actors;
		this.directors = directors;
	}

	private Integer mid;

	private String name;

	private String descri;

	private String timelong;

	private String issue;

	private String shoot;

	private String language;
	private String genres;
	private String actors;
	private String directors;

	public Integer getMid() {
		return mid;
	}

	public void setMid(Integer mid) {
		this.mid = mid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescri() {
		return descri;
	}

	public void setDescri(String descri) {
		this.descri = descri;
	}

	public String getTimelong() {
		return timelong;
	}

	public void setTimelong(String timelong) {
		this.timelong = timelong;
	}

	public String getIssue() {
		return issue;
	}

	public void setIssue(String issue) {
		this.issue = issue;
	}

	public String getShoot() {
		return shoot;
	}

	public void setShoot(String shoot) {
		this.shoot = shoot;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getGenres() {
		return genres;
	}

	public void setGenres(String genres) {
		this.genres = genres;
	}

	public String getActors() {
		return actors;
	}

	public void setActors(String actors) {
		this.actors = actors;
	}

	public String getDirectors() {
		return directors;
	}

	public void setDirectors(String directors) {
		this.directors = directors;
	}
}
