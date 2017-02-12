package qapps.datastore.local.entity;

import javax.persistence.Id;

public class Person {

	@Id
	public Long id;
	public long pesel;
	public String name;
	public String surname;
	public byte age;

	public String toString() {
		return "Person<" + id + "|" + pesel + "|" + name + "|" + surname + "|"
				+ age + ">";
	}
}
