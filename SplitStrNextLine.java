import java.util.Scanner;
class SplitStrNextLine
{
public static void main (String []args)
{
System.out.println("Enter the string: ");
Scanner ss=new Scanner(System.in);
String s=ss.nextLine();
String[] a=s.split(" ");
for(String b:a)
System.out.println(b);
}
}
