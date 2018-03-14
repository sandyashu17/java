import java.util.Scanner;
class Stringrev
{
public static void main (String []args)
{
System.out.println("Enter the string: ");
Scanner ss=new Scanner(System.in);
String s=ss.next();
char value[]=s.toCharArray(); 
for(int i=value.length-1;i>=0;i--)
{
System.out.print(value[i]);
}
}
}