package gr.codelearn.spring.kafka.generator.bootstrap;

import gr.codelearn.spring.kafka.domain.entity.Courier;
import gr.codelearn.spring.kafka.domain.entity.Customer;
import gr.codelearn.spring.kafka.domain.entity.Restaurant;
import gr.codelearn.spring.kafka.domain.enums.VehicleType;
import gr.codelearn.spring.kafka.generator.service.ReferenceDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReferenceDataInitializer implements ApplicationRunner {

	private final ReferenceDataService referenceDataService;

	@Override
	public void run(ApplicationArguments args) {
		log.info("Initialising reference data...");

		referenceDataService.loadRestaurants(List.of(
				new Restaurant("rest-001", "The Athenian Grill", "12 Syntagma Sq, Athens"),
				new Restaurant("rest-002", "Souvlaki Corner", "5 Monastiraki St, Athens"),
				new Restaurant("rest-003", "Gyros & More", "88 Ermou St, Athens"),
				new Restaurant("rest-004", "Zorba's Taverna", "3 Plaka Alley, Athens"),
				new Restaurant("rest-005", "Mediterranean Bites", "21 Kolonaki Blvd, Athens"),
				new Restaurant("rest-006", "Olive Branch Bistro", "9 Kifisias Ave, Athens"),
				new Restaurant("rest-007", "Piraeus Fish House", "1 Harbour Rd, Piraeus"),
				new Restaurant("rest-008", "The Thessaloniki Wrap", "14 Tsimiski St, Thessaloniki"),
				new Restaurant("rest-009", "Santorini Sweets", "7 Sunset Lane, Santorini"),
				new Restaurant("rest-010", "Crete Olive Garden", "33 Knossos Rd, Heraklion")
		                                            ));

		referenceDataService.loadCustomers(List.of(
				new Customer("cust-001", "Nikos Papadopoulos", "nikos.p@example.gr"),
				new Customer("cust-002", "Maria Georgiou", "maria.g@example.gr"),
				new Customer("cust-003", "Alexandros Stavros", "alex.s@example.gr"),
				new Customer("cust-004", "Eleni Konstantinou", "eleni.k@example.gr"),
				new Customer("cust-005", "Dimitrios Alexiou", "dimi.a@example.gr"),
				new Customer("cust-006", "Sofia Nikolaou", "sofia.n@example.gr"),
				new Customer("cust-007", "Kostas Papageorgiou", "kostas.p@example.gr"),
				new Customer("cust-008", "Anna Theodorou", "anna.t@example.gr"),
				new Customer("cust-009", "Petros Karamanlis", "petros.k@example.gr"),
				new Customer("cust-010", "Ioanna Papadimitriou", "ioanna.p@example.gr"),
				new Customer("cust-011", "Vasilis Christodoulou", "vasilis.c@example.gr"),
				new Customer("cust-012", "Katerina Oikonomou", "katerina.o@example.gr"),
				new Customer("cust-013", "Giorgos Andreou", "giorgos.a@example.gr"),
				new Customer("cust-014", "Despina Makris", "despina.m@example.gr"),
				new Customer("cust-015", "Thanasis Vlachos", "thanasis.v@example.gr"),
				new Customer("cust-016", "Irini Fountoulaki", "irini.f@example.gr"),
				new Customer("cust-017", "Panagiotis Deligiannidis", "panagiotis.d@example.gr"),
				new Customer("cust-018", "Stavroula Katsarou", "stavroula.k@example.gr"),
				new Customer("cust-019", "Michalis Economou", "michalis.e@example.gr"),
				new Customer("cust-020", "Theodora Spanos", "theodora.s@example.gr"),
				new Customer("cust-021", "Leonidas Pappas", "leonidas.p@example.gr"),
				new Customer("cust-022", "Chrysoula Tsakalos", "chrysoula.t@example.gr"),
				new Customer("cust-023", "Antonis Mitropoulos", "antonis.m@example.gr"),
				new Customer("cust-024", "Evangelia Sotiropoulou", "evangelia.s@example.gr"),
				new Customer("cust-025", "Stratos Karagiannis", "stratos.k@example.gr"),
				new Customer("cust-026", "Marilena Tzanidaki", "marilena.t@example.gr"),
				new Customer("cust-027", "Fotis Anagnostopoulos", "fotis.a@example.gr"),
				new Customer("cust-028", "Zoe Hatzidimitriou", "zoe.h@example.gr"),
				new Customer("cust-029", "Ilias Papathanasiou", "ilias.p@example.gr"),
				new Customer("cust-030", "Natalia Kouros", "natalia.k@example.gr"),
				new Customer("cust-031", "Spyros Logothetis", "spyros.l@example.gr"),
				new Customer("cust-032", "Vicky Stamou", "vicky.s@example.gr"),
				new Customer("cust-033", "Christos Lagoudakis", "christos.l@example.gr"),
				new Customer("cust-034", "Penelope Daskalaki", "penelope.d@example.gr"),
				new Customer("cust-035", "Aggelos Tzortzis", "aggelos.t@example.gr"),
				new Customer("cust-036", "Rania Papagiannopoulou", "rania.p@example.gr"),
				new Customer("cust-037", "Nikos Anastasiadis", "nikos.a@example.gr"),
				new Customer("cust-038", "Eleftheria Vasileiou", "eleftheria.v@example.gr"),
				new Customer("cust-039", "Manolis Skordilis", "manolis.s@example.gr"),
				new Customer("cust-040", "Thalia Markou", "thalia.m@example.gr"),
				new Customer("cust-041", "Stelios Hatzigeorgiou", "stelios.h@example.gr"),
				new Customer("cust-042", "Artemis Papadaki", "artemis.p@example.gr"),
				new Customer("cust-043", "Yiannis Fragkos", "yiannis.f@example.gr"),
				new Customer("cust-044", "Lamprini Tsiolis", "lamprini.t@example.gr"),
				new Customer("cust-045", "Tasos Argyropoulos", "tasos.a@example.gr"),
				new Customer("cust-046", "Melina Chatzikyriakos", "melina.c@example.gr"),
				new Customer("cust-047", "Kostas Triantafyllou", "kostas.t@example.gr"),
				new Customer("cust-048", "Dora Mavridou", "dora.m@example.gr"),
				new Customer("cust-049", "Aris Sioulis", "aris.s@example.gr"),
				new Customer("cust-050", "Foteini Alexopoulou", "foteini.a@example.gr")
		                                          ));

		referenceDataService.loadCouriers(List.of(
				new Courier("cour-001", "Petros Manolas", VehicleType.MOTORCYCLE),
				new Courier("cour-002", "Yiannis Stamatis", VehicleType.BICYCLE),
				new Courier("cour-003", "Thanasis Katsaros", VehicleType.SCOOTER),
				new Courier("cour-004", "Christos Lazaridis", VehicleType.MOTORCYCLE),
				new Courier("cour-005", "Giorgos Metaxas", VehicleType.CAR)
		                                         ));

		log.info("Reference data loaded: {} restaurants, {} customers, {} couriers",
		         referenceDataService.restaurantCount(),
		         referenceDataService.customerCount(),
		         referenceDataService.courierCount());
	}
}
