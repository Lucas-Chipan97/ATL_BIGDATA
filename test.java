// Classe principale contenant une méthode à tester
public class test {

    // Méthode qui additionne deux nombres
    public int add(int a, int b) {
        return a + b;
    }

    // Méthode principale pour exécuter le programme
    public static void main(String[] args) {
        System.out.println("Bienvenue dans le programme de test !");
        test calculator = new test();
        System.out.println("Résultat de 2 + 3 : " + calculator.add(2, 3));
    }
}
