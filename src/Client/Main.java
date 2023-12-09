package Client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Scanner;

import ShoppingList.CCounter;
import ShoppingList.ShoppingListCRDT;
import Utils.Serializer;

public class Main {

    private static final Scanner scan = new Scanner(System.in);
    private final Client client = new Client();

    private void createShoppingList() throws IOException {
        System.out.println("What name should we give to your shopping list?");
        String listName = scan.nextLine();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime now = LocalDateTime.now();
        String listLink = listName + "-" + dtf.format(now); // TODO: check cloud for repeated link
        System.out.println("From now on you can access this list by providing the following link " + listLink);
        this.editList(new ShoppingListCRDT(), listLink);
    }

    private void getShoppingList() throws IOException {
        System.out.println("What's the link of the shopping list you wish to access?");
        String listLink = scan.nextLine();
        ShoppingListCRDT shoppingListCRDT = parseShoppingList(listLink);
        if (shoppingListCRDT == null) { // shopping list was not found
            System.out.println("Shopping list " + listLink + " was not found");
            return;
        }
        this.editList(shoppingListCRDT, listLink);
    }

    private ShoppingListCRDT parseShoppingList(String listLink) throws IOException {
        String shoppingListPath = new File("src/Client/shopping-lists/" + listLink + ".txt").getAbsolutePath();
        File shoppingListFile = new File(shoppingListPath);
        ShoppingListCRDT shoppingList;

        if (shoppingListFile.exists() ) {
            String content = Files.readString(shoppingListFile.toPath());
            shoppingList = (ShoppingListCRDT) Serializer.deserializeBase64(content);
        } else {
            shoppingList = client.getList(listLink);
            if (shoppingList != null) {
                shoppingList.createNewID();
            }
        }
        return shoppingList;
    }

    private void displayShoppingList(Map<String, CCounter> shoppingList) {
        if (shoppingList.isEmpty()) {
            System.out.println("\nYour shopping list is currently empty");
        } else {
            System.out.println("\nThis is your shopping list");
        }
        for (String item: shoppingList.keySet()) {
            System.out.println(item + ": " + shoppingList.get(item).getItemQuantity());
        }
    }

    private void editList(ShoppingListCRDT shoppingListCRDT, String listLink) throws IOException {
        this.displayShoppingList(shoppingListCRDT.getShoppingList());

        int option = -1;
        while (option != 0) {
            System.out.println("\nWhat operation would you like to do?");
            System.out.println("1 - Add shopping item");
            if (!shoppingListCRDT.getShoppingList().isEmpty()) {
                System.out.println("2 - Remove shopping item");
                System.out.println("3 - Edit quantity to shop for");
                System.out.println("4 - Pull updates from cloud");
                System.out.println("5 - Push updates to cloud");
            }
            else {
                System.out.println("4 - Pull updates from cloud");
            }
            System.out.println("0 - Exit");

            option = scan.nextInt(); scan.nextLine();
            switch (option) {
                case 1 -> this.addListItem(shoppingListCRDT);
                case 2 -> {
                    if (!shoppingListCRDT.getShoppingList().isEmpty()) {
                        this.removeListItem(shoppingListCRDT);
                    } else {
                        System.out.println("\nEmpty list! Invalid Option");
                        option = -1;
                    }
                }
                case 3 -> {
                    if (!shoppingListCRDT.getShoppingList().isEmpty()) {
                        this.updateListItemQuantity(shoppingListCRDT);
                    } else {
                        System.out.println("\nEmpty list! Invalid Option");
                        option = -1;
                    }
                }
                case 4 -> {
                    ShoppingListCRDT shoppingList2 = this.client.getList(listLink);
                    if (shoppingList2 != null){
                        shoppingListCRDT.merge(shoppingList2);
                        System.out.println("Your remote shopping list has been successfully pulled");
                    } else {
                        System.out.println("Shopping list " + listLink + " doesn't exist on the cloud");
                        option = -1;
                    }
                }
                case 5 -> {
                    boolean sent = this.client.pushList(shoppingListCRDT, listLink);
                    if(!sent) {
                        System.out.println("Error sending your shopping list.");
                        option = -1;
                    }
                    else {
                        System.out.println("List " + listLink + " was successfully sent.");
                    }
                }
                case 0 -> System.out.println("\nThank you for using our system");
                default -> {
                    System.out.println("\nInvalid Option");
                    option = -1;
                }
            }

            if (option != 0 && option != -1) {
                this.displayShoppingList(shoppingListCRDT.getShoppingList());
            }
        }
        //save local copy
        String newContent = Serializer.serializeBase64(shoppingListCRDT);
        Path shoppingListPath = Path.of("src/Client/shopping-lists/" + listLink + ".txt");
        Files.writeString(shoppingListPath, newContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void addListItem(ShoppingListCRDT shoppingListCRDT) {
        System.out.println("\nWhat's the name of the item you wish to add to the list?");
        String item = scan.nextLine().toLowerCase();
        if (shoppingListCRDT.getShoppingList().containsKey(item)) {
            System.out.println("This item is already on the list");
            return;
        }
        int quantity = 0;
        while (quantity <= 0) {
            System.out.println("What quantity do you wish to associate the item to?");
            quantity = scan.nextInt(); scan.nextLine();
            if (quantity <= 0) {
                System.out.println("You need to add at least 1 unit of this item");
            }
        }

        shoppingListCRDT.add(item, quantity);
        System.out.println(quantity + " units of " + item + " were added to the list");
    }

    private void removeListItem(ShoppingListCRDT shoppingListCRDT) {
        System.out.println("\nWhat's the name of the item you wish to remove from the list?");
        String item = scan.nextLine().toLowerCase();
        if (!shoppingListCRDT.getShoppingList().containsKey(item)) {
            System.out.println("This item isn't on the list");
            return;
        }

        shoppingListCRDT.remove(item);
        System.out.println(item + " was successfully deleted from the list");
    }

    private void updateListItemQuantity(ShoppingListCRDT shoppingListCRDT) {
        System.out.println("\nWhat's the name of the item whose quantity you wish to update?");
        String item = scan.nextLine().toLowerCase();
        if (!shoppingListCRDT.getShoppingList().containsKey(item)) {
            System.out.println("This item isn't on the list");
            return;
        }

        int option = -1;
        while (option == -1) {
            System.out.println("Do you wish to increment or decrement the quantity of this item?");
            System.out.println("1 - Increment");
            System.out.println("2 - Decrement");

            option = scan.nextInt(); scan.nextLine();
            if (option != 1 && option != 2) {
                System.out.println("Invalid Option");
                option = -1;
                continue;
            }

            int quantity = 0;
            while (quantity <= 0) {
                System.out.println(option == 1 ?
                        "In how many units do you want to increment the quantity of " + item + "?" :
                        "In how many units do you want to decrement the quantity of " + item + "?"
                );
                quantity = scan.nextInt(); scan.nextLine();
                if (quantity <= 0) {
                    System.out.println("You need to specify at least 1 unit of this item");
                }
            }

            switch (option) {
                case 1 -> shoppingListCRDT.increment(item, quantity);
                case 2 -> shoppingListCRDT.decrement(item, quantity);
            }
            System.out.println(option == 1 ?
                item + " quantity successfully increased by " + quantity + " units" :
                item + " quantity successfully decreased by " + quantity + " units"
            );
        }
    }

    public static void main(String[] args) throws IOException {
        Main main = new Main();
        int option = -1;
        while (option == -1) {
            System.out.println("Welcome to your shopping list app!");
            System.out.println("Please select an operation:");
            System.out.println("1 - Create new shopping list");
            System.out.println("2 - Open shopping list");

            option = scan.nextInt(); scan.nextLine();
            switch (option) {
                case 1 -> main.createShoppingList();
                case 2 -> main.getShoppingList();
                default -> {
                    System.out.println("Invalid Option");
                    option = -1;
                }
            }
        }
    }
}