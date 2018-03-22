import java.util.Scanner;
class Calculator
{
public static void main(String []args)
{
float c;
System.out.println("Enter the first number:");
Scanner ss=new Scanner(System.in);
int a=ss.nextInt();
System.out.println("Enter the second number:");
Scanner si=new Scanner(System.in);
int b=si.nextInt();
System.out.println("Enter the option to perform:");
System.out.println("Menu:");
System.out.println("1. Addition");
System.out.println("2. Subtraction");
System.out.println("3. Multiplication");
System.out.println("4. Division");
Scanner sc=new Scanner(System.in);
int option=sc.nextInt();
switch(option)
{
case 1:
c=a+b;
System.out.println("Addition is "+c);
break;
case 2:
c=a-b;
System.out.println("Subtraction is "+c);
break;
case 3:
c=a*b;
System.out.println("Multiplication is "+c);
break;
case 4:
c=a/b;
System.out.println("Division is "+c);
break;
default:
System.out.println("Invalid option");
}
}
}
