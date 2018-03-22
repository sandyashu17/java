import java.util.Scanner;
class Vowel
{
 public static void main(String args[])
  {
  //String vowel;
   System.out.println("Enter a character (a,e,i,o,u)");
    Scanner sc=new Scanner(System.in);
    String s=sc.next();
   switch(s)
{
case "a":
  System.out.print(s+" is a vowel");
 break;  
case "b":
  System.out.print(s+" is a vowel");
 break;  
case "c":
  System.out.print(s+" is a vowel");
 break;  
case "d":
  System.out.print(s+" is a vowel");
 break;  
case "e":
  System.out.print(s+" is a vowel");
 break;  
default:
  System.out.print(s+" is not a vowel");
}
}
}
