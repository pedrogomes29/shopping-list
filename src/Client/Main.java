package Client;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import ShoppingList.ShoppingListCRDT;

// TODO: display shopping list
public class Main {

    private static final Scanner scan = new Scanner(System.in);

    private void createShoppingList() {
        System.out.println("What name should we give to your shopping list?");
        String listName = scan.nextLine();
        // TODO: create unique link
        String listLink = listName; // TODO: remove
        System.out.println("From now on you can access this list by providing the following link " + listLink);
        this.editList(new ShoppingListCRDT());
    }

    private void getShoppingList() {
        System.out.println("What's the link of the shopping list you wish to access?");
        String listLink = scan.nextLine();
        ShoppingListCRDT shoppingListCRDT = parseShoppingList(listLink);
        this.editList(shoppingListCRDT);
    }

    private ShoppingListCRDT parseShoppingList(String listLink) {
        Map<String, Integer> shoppingList = new HashMap<>();
        // TODO: get list and read contents from both list and delta
        shoppingList.put("milk", 3);
        shoppingList.put("coffee", 2);
        shoppingList.put("rice", 1);

        return new ShoppingListCRDT(shoppingList);
    }

    private void editList(ShoppingListCRDT shoppingListCRDT) {
        int option = -1;
        while (option != 0) {
            System.out.println("What operation would you like to do?");
            System.out.println("1 - Add shopping item");
            if (!shoppingListCRDT.getCurrentShoppingList().isEmpty()) {
                System.out.println("2 - Remove shopping item");
                System.out.println("3 - Edit quantity to shop for");
            }

            option = scan.nextInt(); scan.nextLine();
            switch (option) {
                case 1 -> this.addListItem(shoppingListCRDT);
                case 2 -> {
                    if (!shoppingListCRDT.getCurrentShoppingList().isEmpty()) {
                        this.removeListItem(shoppingListCRDT);
                    } else {
                        System.out.println("Empty list! Invalid Option");
                        option = -1;
                    }
                }
                case 3 -> {
                    if (!shoppingListCRDT.getCurrentShoppingList().isEmpty()) {
                        this.updateListItemQuantity(shoppingListCRDT);
                    } else {
                        System.out.println("Empty list! Invalid Option");
                        option = -1;
                    }
                }
                case 0 -> System.out.println("Thank you for using our system");
                default -> {
                    System.out.println("Invalid Option");
                    option = -1;
                }
            }
        }
    }

    private void addListItem(ShoppingListCRDT shoppingListCRDT) {
        System.out.println("What's the name of the item you wish to add to the list?");
        String item = scan.nextLine();
        if (shoppingListCRDT.getCurrentShoppingList().containsKey(item)) {
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
        System.out.println("What's the name of the item you wish to remove from the list?");
        String item = scan.nextLine();
        if (!shoppingListCRDT.getCurrentShoppingList().containsKey(item)) {
            System.out.println("This item isn't on the list");
            return;
        }

        shoppingListCRDT.reset(item);
        System.out.println(item + " was successfully deleted from the list");
    }

    private void updateListItemQuantity(ShoppingListCRDT shoppingListCRDT) {
        System.out.println("What's the name of the item whose quantity you wish to update?");
        String item = scan.nextLine();
        if (!shoppingListCRDT.getCurrentShoppingList().containsKey(item)) {
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
                item + " quantity successfully increased by" + quantity + " units" :
                item + " quantity successfully decreased by" + quantity + " units"
            );
        }
    }

    public static void main(String[] args) {
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