package qapps.datastore.local.entity;

import javax.persistence.Id;

public class Car {

	@Id
	public String rejestracja;
	public String marka, model;

	public String toString() {
		return "Car<" + marka + "|" + model + "|" + rejestracja + ">";
	}
}
