package gr.codelearn.spring.kafka.generator.service;

import gr.codelearn.spring.kafka.domain.entity.Courier;
import gr.codelearn.spring.kafka.domain.entity.Customer;
import gr.codelearn.spring.kafka.domain.entity.Restaurant;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class ReferenceDataService {

	private static final List<String> MENU_ITEMS = List.of(
			"Souvlaki Pita", "Gyros Plate", "Margherita Pizza", "BBQ Burger",
			"Greek Salad", "Tiramisu", "Falafel Wrap", "Moussaka",
			"Tzatziki & Pita", "Seafood Pasta", "Baklava",
			"Sparkling Water", "Fresh Orange Juice");

	private List<Restaurant> restaurants = Collections.emptyList();
	private List<Customer> customers = Collections.emptyList();
	private List<Courier> couriers = Collections.emptyList();

	public void loadRestaurants(List<Restaurant> data) {
		restaurants = List.copyOf(data);
	}

	public void loadCustomers(List<Customer> data) {
		customers = List.copyOf(data);
	}

	public void loadCouriers(List<Courier> data) {
		couriers = List.copyOf(data);
	}

	public int restaurantCount() {
		return restaurants.size();
	}

	public int customerCount() {
		return customers.size();
	}

	public int courierCount() {
		return couriers.size();
	}

	public Restaurant randomRestaurant() {
		return restaurants.get(ThreadLocalRandom.current().nextInt(restaurants.size()));
	}

	public Customer randomCustomer() {
		return customers.get(ThreadLocalRandom.current().nextInt(customers.size()));
	}

	public Courier randomCourier() {
		return couriers.get(ThreadLocalRandom.current().nextInt(couriers.size()));
	}

	public List<String> randomItems() {
		var rng = ThreadLocalRandom.current();
		int count = rng.nextInt(1, 5);
		var shuffled = new ArrayList<>(MENU_ITEMS);
		Collections.shuffle(shuffled, rng);
		return List.copyOf(shuffled.subList(0, count));
	}
}
