import java.util.Scanner;
class Greatest
{
public static void main(String []args)
{
int max=0,n=0;
System.out.println("Enter the numbers");
Scanner ss=new Scanner(System.in);
for(int i=0;i<10;)
{
n=ss.nextInt();
if(n>max)
{
max=n;
}
i++;
}
System.out.println(max+" is the greates of all the numbers");
}
}